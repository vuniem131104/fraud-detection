"""Locust load test for the Fraud Detection Web API.

Targets two endpoints:
  - GET /health  (weight=1)  — lightweight liveness probe
  - POST /score  (weight=5)  — fraud scoring (the primary workload)

Usage (requires a running API server at http://localhost:8000):

    # Start the mock server first (see scripts/mock_server.py)
    python scripts/mock_server.py &

    # Run with Locust UI:
    locust -f tests/locust_load_test.py --host http://localhost:8000

    # Run headless and generate HTML SLA report:
    locust -f tests/locust_load_test.py --headless \\
        --users 10 --spawn-rate 2 --run-time 60s \\
        --html tests/locust_report.html \\
        --host http://localhost:8000

SLA Targets (verified in the HTML report):
  - p50 response time ≤ 200 ms
  - p95 response time ≤ 500 ms
  - Error rate        ≤ 1%
  - Throughput        ≥ 10 req/s at 10 concurrent users
"""

from __future__ import annotations

import json
import os

from locust import HttpUser, between, events, task

# ---------------------------------------------------------------------------
# Realistic scoring payload (matches FraudDetectionInputs schema)
# ---------------------------------------------------------------------------

_SCORE_PAYLOAD = {
    "tx_id": "tx-load-001",
    "event_timestamp": "2017-12-15T13:30:00",
    "amount_usd": 99.50,
    "channel": "W",
    "user_id": "load-user-1",
    "card_id": "0" * 31 + "1",
    "card_country": 840,
    "issuer_code": 84001,
    "card_brand": "visa",
    "bin_code": "411111",
    "card_type": "credit",
    "billing_zone": 1,
    "billing_country": 840,
    "email_purchaser": "buyer@gmail.com",
    "email_recipient": "seller@example.com",
    "device_type": "desktop",
    "device_info": "desktop:Windows 11:Chrome",
    "os_raw": "Windows 11",
    "browser_raw": "Chrome 120",
    "screen_resolution": "1920x1080",
    "C1": 3,
    "C2": 2,
    "C13": 5,
    "D4": 10.0,
    "D15": 3.0,
    "M1": "T",
    "M2": "T",
    "M6": "F",
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
    host = os.getenv("LOCUST_HOST", "http://localhost:8000")

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
            json=_SCORE_PAYLOAD,
            headers={"Content-Type": "application/json"},
            catch_response=True,
            name="POST /score",
        ) as resp:
            if resp.status_code == 200:
                try:
                    body = resp.json()
                    if "probability" in body and "prediction" in body:
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

    rps = total.current_rps
    if rps >= 10:
        passed.append(f"✓ Throughput {rps:.1f} req/s ≥ 10 req/s")
    else:
        passed.append(f"~ Throughput {rps:.1f} req/s (check HTML for full run stats)")

    p50 = total.get_response_time_percentile(0.50) or 0
    p95 = total.get_response_time_percentile(0.95) or 0

    if p50 <= 200:
        passed.append(f"✓ p50 latency {p50:.0f} ms ≤ 200 ms")
    else:
        failures.append(f"✗ p50 latency {p50:.0f} ms > 200 ms")

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
