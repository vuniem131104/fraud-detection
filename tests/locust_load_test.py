"""Locust load test for the Fraud Detection Web API.

Targets two endpoints:
  - GET /health  (weight=1)  — lightweight liveness probe
  - POST /score  (weight=5)  — fraud scoring (the primary workload)

Usage (against the real service, port-forwarded from the cluster):

    kubectl port-forward -n core svc/fraud-detection 1311:80

    # Run with the Locust UI:
    locust -f tests/locust_load_test.py --host http://localhost:1311

    # Run headless and generate the HTML SLA report:
    locust -f tests/locust_load_test.py --headless \\
        --users 10 --spawn-rate 2 --run-time 60s \\
        --html proof/validation_verification/locust_report.html \\
        --host http://localhost:1311

SLA Targets (verified in the HTML report and asserted on exit):
  - p50 response time ≤ 350 ms
  - p95 response time ≤ 500 ms
  - Error rate        ≤ 1%
  - Throughput        ≥ 10 req/s at 10 concurrent users

Note on the latency targets: the test client reaches the cluster through a
kubectl port-forward tunnel whose measured keep-alive round-trip floor is
~205 ms (GET /health, a static return, has a 203 ms *minimum*). The latency
SLAs are therefore end-to-end targets over that tunnel; the server-side
scoring cost itself is the ~65 ms delta between /score and /health medians.
"""

from __future__ import annotations

import json
import os
import random
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from locust import HttpUser, between, events, task

HO_CHI_MINH_TZ = timezone(timedelta(hours=7), "Asia/Ho_Chi_Minh")

# ---------------------------------------------------------------------------
# Real (user_id, card_id) pairs materialised in the online feature store
# (from DB: SELECT DISTINCT user_id, card_id FROM application.transactions)
# ---------------------------------------------------------------------------

REAL_USER_CARDS = [
    {"user_id": "1fa284b749d74bf7b21a4876fe403127", "card_id": "0002594193c74a24a5aa6a2fc52df73d"},
    {"user_id": "854780b237bd4aa9a540261fd6ac8d02", "card_id": "0002d07723a84c85821cf803fa5c9a9a"},
    {"user_id": "bd61c875867749a8b799b4993554f632", "card_id": "0004550dd62f44aca3805b1584854e39"},
    {"user_id": "73d5587ca4634dc696ef41377802649a", "card_id": "0005063a5e894c6ba1de2dd8679d4746"},
    {"user_id": "62ddd14b60f34fcbbaeddcb6740ed36a", "card_id": "0007ee8989734244a7e2095f51813f90"},
    {"user_id": "24280a3a8c624783b7c3bb192da7e5ca", "card_id": "000ab5cf43f441fe82ecadbfc83cf617"},
    {"user_id": "81796a68ae3f4ed192daf81ef8b7bdd1", "card_id": "000ae491838640a5bc7680b3551c34f1"},
    {"user_id": "d7b4b99c44cc4bcab75d54e39142994d", "card_id": "000bae4059bf47c68338c84213f9a896"},
    {"user_id": "70feeb686d914530bb11e816bc80b81c", "card_id": "000ce9f2ac8845689c45deaeb8d873d1"},
    {"user_id": "70474cd4d3564d62a41e6111fb2eeda7", "card_id": "000d8dd71ab8446e9e50c568473dd51a"},
    {"user_id": "fabdb85fc95b4b9786211ffc66e53e2b", "card_id": "000e8c07a6f84db58540803231b60382"},
    {"user_id": "20438a1489f8426ea5858a1ea51a69e7", "card_id": "000ed856fceb486bbc681917f9e54dbd"},
    {"user_id": "ea49b3703eaa43ffb504c3b81a1756bc", "card_id": "000fd4101181428f9616066ab811f943"},
    {"user_id": "59d543b1bfba4df2a671c227a87683fb", "card_id": "0012d170daa04634b0839af55ae091cd"},
    {"user_id": "f0c6fa4cd045475981622f85baf700cc", "card_id": "00150974114b439c8443daf5e38c19a0"},
    {"user_id": "73f3d1e88d244c64a6950651e5bf4a3c", "card_id": "00191e56f3b64b19b86371999fe57f9a"},
    {"user_id": "409368720bb14b67b3951cc13939b19f", "card_id": "00191f4fc6f4485ca6ed05f1b8efbf10"},
    {"user_id": "b9960c55b76c4c19ad602ec4e1a45e4d", "card_id": "001a9a9e115a42ed9cea4ee15f7576ba"},
    {"user_id": "b13964fb386f4dabb75a019eaa360284", "card_id": "001b7100b7b94f49abc1608851b56fec"},
    {"user_id": "061e8b42795442768ac69211c4cdd78b", "card_id": "001b9594ec82466d922c92283f7d9a8f"},
]


def build_score_payload() -> dict:
    """A realistic low-risk transaction matching ``FraudDetectionInputs``."""
    pair = random.choice(REAL_USER_CARDS)
    return {
        "transaction_id": uuid4().hex,
        "user_id": pair["user_id"],
        "card_id": pair["card_id"],
        "merchant_category": random.choice(["retail", "grocery", "travel", "restaurant"]),
        "merchant_risk_level": random.randint(0, 3),
        "amount_usd": round(random.uniform(20, 100), 2),
        "timestamp": datetime.now(HO_CHI_MINH_TZ).replace(microsecond=0).isoformat(),
        "channel": random.choice(["web", "mobile", "pos"]),
        "billing_country_code": "VN",
        "ip_country_code": "VN",
        "email_purchaser": "buyer@gmail.com",
        "email_recipient": "seller@gmail.com",
    }


# ---------------------------------------------------------------------------
# Locust User
# ---------------------------------------------------------------------------

class FraudApiUser(HttpUser):
    """Simulates a client sending scoring requests to the Fraud Detection API.

    Waits between 0.1 and 1.0 seconds between requests to simulate realistic
    client pacing. The /score endpoint is called 5× more often than /health,
    reflecting production traffic patterns.
    """

    wait_time = between(0.1, 1.0)
    host = os.getenv("LOCUST_HOST", "http://localhost:1311")

    @task(1)
    def get_health(self) -> None:
        """Liveness probe — lightweight, called infrequently."""
        with self.client.get("/health", catch_response=True, name="GET /health") as resp:
            if resp.status_code == 200:
                body = resp.json()
                if body.get("status") == "ok":
                    resp.success()
                else:
                    resp.failure(f"Unexpected health body: {body}")
            else:
                resp.failure(f"HTTP {resp.status_code}")

    @task(5)
    def post_score(self) -> None:
        """Transaction scoring — the primary load-generating endpoint."""
        with self.client.post(
            "/score",
            json=build_score_payload(),
            headers={"Content-Type": "application/json"},
            catch_response=True,
            name="POST /score",
        ) as resp:
            if resp.status_code == 200:
                try:
                    body = resp.json()
                    if "probability" in body and "prediction" in body and "transaction_id" in body:
                        resp.success()
                    else:
                        resp.failure(f"Missing fields in response: {body}")
                except (json.JSONDecodeError, ValueError) as exc:
                    resp.failure(f"Invalid JSON: {exc}")
            elif resp.status_code == 422:
                # Validation errors indicate a test misconfiguration, not a server issue
                resp.failure(f"Validation error (422): {resp.text}")
            elif resp.status_code == 503:
                resp.failure("Service unavailable (503)")
            else:
                resp.failure(f"Unexpected status {resp.status_code}: {resp.text[:200]}")


# ---------------------------------------------------------------------------
# SLA assertions reported at the end of the run
# ---------------------------------------------------------------------------

@events.quitting.add_listener
def assert_sla(environment, **kwargs):
    """Emit SLA summary and set exit code 1 if any SLA is violated."""
    stats = environment.stats

    failures: list[str] = []
    passed: list[str] = []

    total = stats.total
    if total.num_requests == 0:
        print("[SLA] No requests made — skipping SLA check.")
        return

    error_rate = total.num_failures / total.num_requests * 100
    if error_rate <= 1.0:
        passed.append(f"✓ Error rate {error_rate:.2f}% ≤ 1%")
    else:
        failures.append(f"✗ Error rate {error_rate:.2f}% > 1%")

    rps = total.total_rps
    if rps >= 10:
        passed.append(f"✓ Throughput {rps:.1f} req/s ≥ 10 req/s")
    else:
        failures.append(f"✗ Throughput {rps:.1f} req/s < 10 req/s")

    p50 = total.get_response_time_percentile(0.50) or 0
    p95 = total.get_response_time_percentile(0.95) or 0

    if p50 <= 350:
        passed.append(f"✓ p50 latency {p50:.0f} ms ≤ 350 ms")
    else:
        failures.append(f"✗ p50 latency {p50:.0f} ms > 350 ms")

    if p95 <= 500:
        passed.append(f"✓ p95 latency {p95:.0f} ms ≤ 500 ms")
    else:
        failures.append(f"✗ p95 latency {p95:.0f} ms > 500 ms")

    print("\n" + "=" * 60)
    print("SLA REPORT")
    print("=" * 60)
    for msg in passed:
        print(msg)
    for msg in failures:
        print(msg)
    print("=" * 60)

    if failures:
        environment.process_exit_code = 1
        print(f"FAILED: {len(failures)} SLA violation(s)")
    else:
        print("PASSED: All SLA targets met")
