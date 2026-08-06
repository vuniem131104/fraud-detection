"""
load_test_autoscale.py – Demonstrate HPA autoscaling by hammering the fraud-detection API.

The script bypasses the Ingress rate-limit by talking directly to the ClusterIP Service
(via `kubectl port-forward`) or to the Ingress host with multiple concurrent workers.

Usage:
    # Option A – port-forward (bypasses nginx rate-limit):
    kubectl port-forward svc/fraud-detection 8080:80 -n core &
    python3 scripts/load_test_autoscale.py --url http://localhost:8080 --workers 40 --duration 180

    # Option B – hit the Ingress directly (rate-limited to 10 RPS per IP):
    python3 scripts/load_test_autoscale.py \\
        --url http://fraud-detection-api.34.56.166.63.sslip.io \\
        --user admin --password <your-password> \\
        --workers 20 --duration 180

    # Option C – watch HPA in a separate terminal while running the test:
    watch -n 2 'kubectl get hpa,pods -n core'

Environment variables (all optional, override via CLI flags):
    API_URL        Base URL of the fraud-detection service
    API_USER       HTTP basic-auth username
    API_PASSWORD   HTTP basic-auth password
"""

from __future__ import annotations

import argparse
import os
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Event, Lock
from uuid import uuid4

import httpx

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]
HO_CHI_MINH_TZ = timezone(timedelta(hours=7), "Asia/Ho_Chi_Minh")

DEFAULT_URL = "http://localhost:8080"
DEFAULT_WORKERS = 30
DEFAULT_DURATION = 180  # seconds
SCORE_ENDPOINT = "/score"


# ---------------------------------------------------------------------------
# Real user/card pool (from DB: SELECT DISTINCT user_id, card_id FROM application.transactions LIMIT 20)
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


# ---------------------------------------------------------------------------
# Payload builders
# ---------------------------------------------------------------------------

def _ts() -> str:
    return datetime.now(HO_CHI_MINH_TZ).replace(microsecond=0).isoformat()


def build_normal_payload() -> dict:
    """Low-risk transaction using a real user/card pair from the DB pool."""
    pair = random.choice(REAL_USER_CARDS)
    return {
        "transaction_id":      uuid4().hex,
        "user_id":             pair["user_id"],
        "card_id":             pair["card_id"],
        "merchant_category":   random.choice(["retail", "grocery", "travel", "restaurant"]),
        "merchant_risk_level": random.randint(0, 3),
        "amount_usd":          round(random.uniform(20, 100), 2),
        "timestamp":           _ts(),
        "channel":             random.choice(["web", "mobile", "pos"]),
        "billing_country_code": "VN",
        "ip_country_code":     "VN",
        "email_purchaser":     f"buyer@gmail.com",
        "email_recipient":     f"seller@gmail.com",
    }


# ---------------------------------------------------------------------------
# Stats collector
# ---------------------------------------------------------------------------

@dataclass
class Stats:
    lock: Lock = field(default_factory=Lock)
    total:     int = 0
    success:   int = 0
    errors:    int = 0
    frauds:    int = 0
    latencies: list = field(default_factory=list)
    start_time: float = field(default_factory=time.monotonic)

    def record(self, *, ok: bool, latency_ms: float, is_fraud: bool = False) -> None:
        with self.lock:
            self.total += 1
            if ok:
                self.success += 1
                self.latencies.append(latency_ms)
                if is_fraud:
                    self.frauds += 1
            else:
                self.errors += 1

    def snapshot(self) -> dict:
        with self.lock:
            elapsed = time.monotonic() - self.start_time
            lats = sorted(self.latencies)
            p50 = lats[int(len(lats) * 0.50)] if lats else 0
            p95 = lats[int(len(lats) * 0.95)] if lats else 0
            p99 = lats[int(len(lats) * 0.99)] if lats else 0
            rps  = self.success / elapsed if elapsed > 0 else 0
            return {
                "elapsed_s": round(elapsed, 1),
                "total":     self.total,
                "success":   self.success,
                "errors":    self.errors,
                "frauds":    self.frauds,
                "rps":       round(rps, 2),
                "p50_ms":    round(p50, 1),
                "p95_ms":    round(p95, 1),
                "p99_ms":    round(p99, 1),
            }


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------

def worker(
    url,
    auth,
    stats,
    stop,
):
    """Continuously POST /score until `stop` is set."""
    client = httpx.Client(
        base_url=url,
        auth=auth,
        timeout=30.0,
        limits=httpx.Limits(max_connections=5, max_keepalive_connections=2),
    )
    try:
        while not stop.is_set():
            payload = build_normal_payload()
            t0 = time.monotonic()
            try:
                resp = client.post(SCORE_ENDPOINT, json=payload)
                latency_ms = (time.monotonic() - t0) * 1000
                ok = resp.status_code < 400
                is_fraud = False
                if ok:
                    try:
                        is_fraud = resp.json().get("prediction", 0) == 1
                    except Exception:
                        pass
                stats.record(ok=ok, latency_ms=latency_ms, is_fraud=is_fraud)
            except Exception:
                latency_ms = (time.monotonic() - t0) * 1000
                stats.record(ok=False, latency_ms=latency_ms)
    finally:
        client.close()


# ---------------------------------------------------------------------------
# Progress printer
# ---------------------------------------------------------------------------

def print_banner(url: str, workers: int, duration: int) -> None:
    print("\n" + "=" * 60)
    print("  🚀  Fraud-Detection Autoscale Load Test")
    print("=" * 60)
    print(f"  Target  : {url}{SCORE_ENDPOINT}")
    print(f"  Workers : {workers} concurrent threads")
    print(f"  Duration: {duration}s")
    print("=" * 60)
    print()
    print("  Tip: in another terminal, run:")
    print("    watch -n 2 'kubectl get hpa,pods -n core'")
    print()


def print_progress(stats: Stats) -> None:
    s = stats.snapshot()
    print(
        f"  [{s['elapsed_s']:>6.1f}s]  "
        f"RPS: {s['rps']:>6.2f}  |  "
        f"OK: {s['success']:>6}  ERR: {s['errors']:>4}  "
        f"FRAUD: {s['frauds']:>5}  |  "
        f"p50: {s['p50_ms']:>6.1f}ms  "
        f"p95: {s['p95_ms']:>6.1f}ms  "
        f"p99: {s['p99_ms']:>6.1f}ms"
    )


def print_summary(stats: Stats) -> None:
    s = stats.snapshot()
    print("\n" + "=" * 60)
    print("  📊  Load Test Summary")
    print("=" * 60)
    print(f"  Duration   : {s['elapsed_s']}s")
    print(f"  Total reqs : {s['total']}")
    print(f"  Successful : {s['success']}  ({s['rps']} RPS avg)")
    print(f"  Errors     : {s['errors']}")
    print(f"  Frauds det.: {s['frauds']}")
    print(f"  Latency p50: {s['p50_ms']} ms")
    print(f"  Latency p95: {s['p95_ms']} ms")
    print(f"  Latency p99: {s['p99_ms']} ms")
    print("=" * 60)
    print()
    print("  Check HPA final state:")
    print("    kubectl get hpa -n core")
    print("    kubectl describe hpa fraud-detection-api -n core")
    print()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Load test to trigger HPA autoscaling on fraud-detection.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument(
        "--url",
        default=os.getenv("API_URL", DEFAULT_URL),
        help=f"Base URL (default: {DEFAULT_URL})",
    )
    p.add_argument(
        "--user",
        default=os.getenv("API_USER", ""),
        help="Basic-auth username (leave empty if using port-forward)",
    )
    p.add_argument(
        "--password",
        default=os.getenv("API_PASSWORD", ""),
        help="Basic-auth password",
    )
    p.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"Number of concurrent worker threads (default: {DEFAULT_WORKERS})",
    )
    p.add_argument(
        "--duration",
        type=int,
        default=DEFAULT_DURATION,
        help=f"Test duration in seconds (default: {DEFAULT_DURATION})",
    )
    p.add_argument(
        "--report-interval",
        type=int,
        default=10,
        dest="report_interval",
        help="Print a progress line every N seconds (default: 10)",
    )
    return p.parse_args()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> int:
    args = parse_args()

    auth = None
    if args.user and args.password:
        auth = (args.user, args.password)

    print_banner(args.url, args.workers, args.duration)

    stats = Stats()
    stop  = Event()

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [
            pool.submit(
                worker,
                args.url,
                auth,
                stats,
                stop,
            )
            for _ in range(args.workers)
        ]

        deadline = time.monotonic() + args.duration
        next_report = time.monotonic() + args.report_interval

        try:
            while time.monotonic() < deadline:
                if time.monotonic() >= next_report:
                    print_progress(stats)
                    next_report += args.report_interval
                time.sleep(0.5)
        except KeyboardInterrupt:
            print("\n  [!] Interrupted by user – stopping workers …")

        stop.set()

        # Drain futures (they exit quickly once stop is set)
        for f in as_completed(futures, timeout=10):
            try:
                f.result()
            except Exception as exc:
                print(f"  Worker exception: {exc}", file=sys.stderr)

    print_progress(stats)
    print_summary(stats)
    return 0


if __name__ == "__main__":
    sys.exit(main())
