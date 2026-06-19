"""
simulate_transactions.py – Simulate historical transaction API calls.

Generates 10 days of transactions (from 15 days ago to 5 days ago).
Each day: 5 000 transactions, sent in concurrent batches of 5.
Around 30–50 of each day's transactions are crafted to look anomalous
(high amount, foreign billing, sketchy device, mismatched email, etc.)
covering ≈50 % of real users per day.

IMPORTANT: Requires the same Postgres env vars as other scripts
(POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB).
Loads real user/card pairs so foreign key constraints are satisfied.

Usage:
    uv run python scripts/simulate_transactions.py [OPTIONS]

Options:
    --api-url       Fraud-detection API endpoint  (default: http://localhost:1311/score)
    --tx-per-day    Transactions per day           (default: 5000)
    --batch-size    Concurrent requests per batch  (default: 5)
    --anomaly-min   Min anomalous tx per day       (default: 30)
    --anomaly-max   Max anomalous tx per day       (default: 50)
    --seed          Random seed                    (default: 77)
    --days-from     Start offset in days ago       (default: 15)
    --days-to       End offset in days ago         (default: 5)
    --dry-run       Print first batch, skip HTTP
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import shutil
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx

# ---------------------------------------------------------------------------
# Path setup – reuse database module from src/
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

try:
    from database.postgres import PostgresDatabase
except ModuleNotFoundError:
    uv_path = shutil.which("uv")
    if uv_path and not os.environ.get("SIMULATE_TX_UV_REEXEC"):
        env = {**os.environ, "SIMULATE_TX_UV_REEXEC": "1"}
        os.execvpe(uv_path, [uv_path, "run", "python", __file__, *sys.argv[1:]], env)
    raise

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

HO_CHI_MINH_TZ = timezone(timedelta(hours=7), "Asia/Ho_Chi_Minh")
DEFAULT_API_URL = "http://localhost:1311/score"
DEFAULT_TX_PER_DAY = 5_000
DEFAULT_BATCH_SIZE = 5
DEFAULT_ANOMALY_MIN = 30
DEFAULT_ANOMALY_MAX = 50
DEFAULT_SEED = 77
DEFAULT_DAYS_FROM = 15
DEFAULT_DAYS_TO = 5

SUSPICIOUS_EMAIL_DOMAINS = [
    "protonmail.com", "temp-mail.org", "throwaway.email",
    "guerrillamail.com", "mailinator.com",
]

ABNORMAL_COUNTRIES = [
    (643, 4),  # RU
    (566, 4),  # NG
    (76,  4),  # BR
    (484, 4),  # MX
    (792, 4),  # TR
]

COUNTRIES = [
    (840, 1), (124, 1),
    (826, 2), (276, 2), (250, 2),
    (704, 3), (702, 3), (392, 3), (410, 3), (36, 3),
]

DEVICE_PROFILES = [
    ("mobile",  "iOS 17",     "Safari Mobile",   "390x844"),
    ("mobile",  "Android 14", "Chrome Mobile",   "412x915"),
    ("desktop", "macOS 14",   "Safari",          "1440x900"),
    ("desktop", "Windows 11", "Chrome",          "1920x1080"),
    ("desktop", "Windows 11", "Edge",            "1366x768"),
]
ANOMALY_DEVICE_PROFILES = [
    ("mobile", "Android 4.4", "unknown browser 0.1", "720x1280"),
    ("mobile", "Android 5.0", "UCWEB 2.0",           "480x800"),
    ("mobile", "Android 6.0", "Opera Mini 3",        "360x640"),
]
CHANNELS = ("W", "C", "R")  # web, mobile_app, pos


# ---------------------------------------------------------------------------
# Postgres helpers
# ---------------------------------------------------------------------------

def _country_zone(country: int) -> int:
    for code, zone in COUNTRIES:
        if code == country:
            return zone
    return 1


async def load_pool_from_postgres(database: PostgresDatabase) -> list[dict]:
    """
    Fetch every (user, card) pair seeded by init_historical_data.py.
    Returns a list of dicts with all fields needed to build payloads.
    """
    async with database.connection() as conn:
        rows = await conn.fetch(
            """
            SELECT
                u.id          AS user_id,
                u.email,
                c.id          AS card_id,
                c.issuer_code,
                c.country     AS card_country,
                c.brand       AS card_brand,
                c.type        AS card_type,
                c.bin_code,
                c.created_at  AS card_created_at
            FROM application.users u
            JOIN application.cards c ON c.user_id = u.id
            ORDER BY u.id, c.id
            """
        )
    pool = []
    for row in rows:
        card_country = int(row["card_country"])
        pool.append(
            {
                "user_id":        row["user_id"],
                "card_id":        row["card_id"],
                "email":          row["email"],
                "issuer_code":    int("".join(ch for ch in str(row["issuer_code"]) if ch.isdigit()) or "0"),
                "card_country":   card_country,
                "card_zone":      _country_zone(card_country),
                "card_brand":     row["card_brand"],
                "card_type":      row["card_type"],
                "bin_code":       row["bin_code"],
                "card_created_at": row["card_created_at"],
            }
        )
    return pool


# ---------------------------------------------------------------------------
# Payload builders
# ---------------------------------------------------------------------------

def _normal_amount(rng: random.Random) -> float:
    amount = rng.lognormvariate(3.35, 0.65)
    return round(min(max(amount, 3.0), 350.0), 2)


def _anomaly_amount(rng: random.Random) -> float:
    return round(rng.uniform(1_800.0, 9_500.0), 2)


def _card_age_days(card: dict, event_ts: datetime) -> float:
    card_created = card["card_created_at"]
    if card_created.tzinfo is None:
        card_created = card_created.replace(tzinfo=HO_CHI_MINH_TZ)
    return round(max((event_ts - card_created).total_seconds() / 86_400, 0.0), 4)


def _email_domain(email: str) -> str:
    return email.split("@", 1)[1].lower() if "@" in email else email.lower()


def build_normal_payload(
    card: dict,
    event_ts: datetime,
    prev_tx_count: int,
    prev_tx_at: datetime,
    rng: random.Random,
) -> dict[str, Any]:
    device_type, os_raw, browser_raw, screen = DEVICE_PROFILES[
        rng.randint(0, len(DEVICE_PROFILES) - 1)
    ]
    billing_country, billing_zone = card["card_country"], card["card_zone"]
    if rng.random() < 0.06:
        same_zone = [
            (c, z) for c, z in COUNTRIES
            if z == card["card_zone"] and c != card["card_country"]
        ]
        if same_zone:
            billing_country, billing_zone = rng.choice(same_zone)

    card_age = _card_age_days(card, event_ts)
    days_since = round(
        max((event_ts - prev_tx_at).total_seconds() / 86_400, 0.0), 4
    )
    domain = _email_domain(card["email"])

    return {
        "tx_id":             uuid4().hex,
        "user_id":           card["user_id"],
        "card_id":           card["card_id"],
        "event_timestamp":   event_ts.isoformat(),
        "amount_usd":        _normal_amount(rng),
        "channel":           rng.choice(CHANNELS),
        "card_country":      card["card_country"],
        "issuer_code":       card["issuer_code"],
        "card_brand":        card["card_brand"],
        "bin_code":          card["bin_code"],
        "card_type":         card["card_type"],
        "billing_zone":      billing_zone,
        "billing_country":   billing_country,
        "email_purchaser":   domain,
        "email_recipient":   domain,
        "device_type":       device_type,
        "device_info":       f"{device_type}:{os_raw}:{browser_raw}",
        "os_raw":            os_raw,
        "browser_raw":       browser_raw,
        "screen_resolution": screen,
        "C1":  rng.randint(1, 6),
        "C2":  rng.randint(1, 4),
        "C13": max(prev_tx_count + 1, 1),
        "D4":  card_age,
        "D15": days_since,
        "M1":  "T",
        "M2":  "T",
        "M6":  "F",
    }


def build_anomaly_payload(
    card: dict,
    event_ts: datetime,
    prev_tx_count: int,
    prev_tx_at: datetime,
    rng: random.Random,
) -> dict[str, Any]:
    """
    Craft payload to push fraud probability higher:
    - Very high amount ($1800–$9500)
    - Billing from high-risk region (zone 4: RU/NG/BR/MX/TR)
    - Old/rooted Android device
    - Recipient is a throwaway address
    - Inflated C-features (card-testing velocity pattern)
    - D15 ≈ 0 (transaction fired immediately after previous one)
    - M1/M2=F (purchaser ≠ recipient), M6=T (suspicious flag)
    """
    device_type, os_raw, browser_raw, screen = ANOMALY_DEVICE_PROFILES[
        rng.randint(0, len(ANOMALY_DEVICE_PROFILES) - 1)
    ]
    billing_country, billing_zone = ABNORMAL_COUNTRIES[
        rng.randint(0, len(ABNORMAL_COUNTRIES) - 1)
    ]
    suspicious_recipient = rng.choice(SUSPICIOUS_EMAIL_DOMAINS)
    card_age = _card_age_days(card, event_ts)
    # Velocity signal: last tx happened just moments ago
    days_since = round(rng.uniform(0.001, 0.05), 4)

    return {
        "tx_id":             uuid4().hex,
        "user_id":           card["user_id"],
        "card_id":           card["card_id"],
        "event_timestamp":   event_ts.isoformat(),
        "amount_usd":        _anomaly_amount(rng),
        "channel":           "C",   # mobile_app – higher risk channel
        "card_country":      card["card_country"],
        "issuer_code":       card["issuer_code"],
        "card_brand":        card["card_brand"],
        "bin_code":          card["bin_code"],
        "card_type":         card["card_type"],
        "billing_zone":      billing_zone,
        "billing_country":   billing_country,
        "email_purchaser":   _email_domain(card["email"]),
        "email_recipient":   suspicious_recipient,
        "device_type":       device_type,
        "device_info":       f"UnknownRooted{device_type} X999",
        "os_raw":            os_raw,
        "browser_raw":       browser_raw,
        "screen_resolution": screen,
        "C1":  rng.randint(30, 55),
        "C2":  rng.randint(25, 45),
        "C13": max(prev_tx_count + rng.randint(70, 110), 90),
        "D4":  card_age,
        "D15": days_since,
        "M1":  "F",
        "M2":  "F",
        "M6":  "T",
    }


# ---------------------------------------------------------------------------
# HTTP sender
# ---------------------------------------------------------------------------

async def send_batch(
    client: httpx.AsyncClient,
    payloads: list[dict],
    api_url: str,
    *,
    dry_run: bool = False,
) -> list[dict]:
    if dry_run:
        for p in payloads:
            print(json.dumps(p, indent=2))
        return [{"tx_id": p["tx_id"], "dry_run": True} for p in payloads]

    async def _post(payload: dict) -> dict:
        try:
            resp = await client.post(api_url, json=payload, timeout=30.0)
            resp.raise_for_status()
            result = resp.json()
            return {
                "tx_id":       payload["tx_id"],
                "probability": result.get("probability"),
                "prediction":  result.get("prediction"),
                "is_anomaly":  payload.get("_is_anomaly", False),
            }
        except Exception as exc:
            return {
                "tx_id":    payload["tx_id"],
                "error":    str(exc),
                "is_anomaly": payload.get("_is_anomaly", False),
            }

    return await asyncio.gather(*[_post(p) for p in payloads])


# ---------------------------------------------------------------------------
# Day simulation
# ---------------------------------------------------------------------------

def _build_day_payloads(
    day_dt: datetime,
    pool: list[dict],
    rng: random.Random,
    tx_per_day: int,
    anomaly_min: int,
    anomaly_max: int,
) -> list[dict]:
    """
    Build tx_per_day payloads for a given calendar day.
    - Picks ~50 % of pool as active users that day.
    - Spreads timestamps across 00:00–23:59.
    - Injects anomaly_count anomalous payloads (one anomaly per card per day).
    """
    active_count = max(len(pool) // 2, 1)
    active_cards = rng.sample(pool, min(active_count, len(pool)))

    anomaly_count = rng.randint(anomaly_min, anomaly_max)
    anomaly_card_ids = {
        c["card_id"]
        for c in rng.sample(active_cards, min(anomaly_count, len(active_cards)))
    }

    day_start = day_dt.replace(hour=0, minute=0, second=0, microsecond=0)

    # Track per-card running state
    prev_tx_at: dict[str, datetime] = {}
    prev_tx_count: dict[str, int] = {}
    anomaly_sent: set[str] = set()  # card_ids already given an anomaly today

    payloads: list[dict] = []
    for _ in range(tx_per_day):
        card = rng.choice(active_cards)
        uid = card["card_id"]

        card_created = card["card_created_at"]
        if card_created.tzinfo is None:
            card_created = card_created.replace(tzinfo=HO_CHI_MINH_TZ)

        event_ts = day_start + timedelta(seconds=rng.randint(0, 86_399))
        p_at = prev_tx_at.get(uid, card_created)
        p_cnt = prev_tx_count.get(uid, rng.randint(3, 20))

        is_anomaly = (uid in anomaly_card_ids) and (uid not in anomaly_sent)

        if is_anomaly:
            p = build_anomaly_payload(card, event_ts, p_cnt, p_at, rng)
            anomaly_sent.add(uid)
        else:
            p = build_normal_payload(card, event_ts, p_cnt, p_at, rng)

        p["_is_anomaly"] = is_anomaly
        prev_tx_at[uid] = event_ts
        prev_tx_count[uid] = p_cnt + 1
        payloads.append(p)

    rng.shuffle(payloads)
    return payloads


# ---------------------------------------------------------------------------
# Main simulation loop
# ---------------------------------------------------------------------------

async def simulate(args: argparse.Namespace) -> int:
    rng = random.Random(args.seed)

    # ------------------------------------------------------------------
    # Load real users/cards from Postgres
    # ------------------------------------------------------------------
    print("Connecting to Postgres to load real user/card pool …", flush=True)
    database = PostgresDatabase.from_env()
    await database.open()
    try:
        pool = await load_pool_from_postgres(database)
    finally:
        await database.close()

    if not pool:
        print("ERROR: No users/cards found in Postgres. Run init_historical_data.py first.", file=sys.stderr)
        return 1

    print(f"Loaded {len(pool)} user/card pairs from Postgres.", flush=True)

    now = datetime.now(HO_CHI_MINH_TZ).replace(microsecond=0)
    days = list(range(args.days_from, args.days_to - 1, -1))  # e.g. [15,14,...,5]
    total_days = len(days)

    grand_total = 0
    grand_errors = 0
    grand_flagged = 0

    limits = httpx.Limits(
        max_connections=args.batch_size + 4,
        max_keepalive_connections=args.batch_size,
    )
    async with httpx.AsyncClient(limits=limits, timeout=30.0) as client:
        for day_idx, days_ago in enumerate(days, start=1):
            day_dt = now - timedelta(days=days_ago)
            day_label = day_dt.strftime("%Y-%m-%d")

            print(
                f"\n[Day {day_idx}/{total_days}] {day_label}  (now-{days_ago}d)",
                flush=True,
            )

            payloads = _build_day_payloads(
                day_dt, pool, rng,
                tx_per_day=args.tx_per_day,
                anomaly_min=args.anomaly_min,
                anomaly_max=args.anomaly_max,
            )

            expected_anomalies = sum(1 for p in payloads if p.get("_is_anomaly"))
            print(
                f"  Prepared {len(payloads)} txs  "
                f"({expected_anomalies} anomalous, "
                f"{len(set(p['user_id'] for p in payloads))} unique users)",
                flush=True,
            )

            # Strip internal marker before sending
            clean_payloads = [
                {k: v for k, v in p.items() if k != "_is_anomaly"}
                for p in payloads
            ]
            is_anomaly_flags = [p["_is_anomaly"] for p in payloads]

            day_total = 0
            day_errors = 0
            day_flagged = 0
            batch_count = (len(clean_payloads) + args.batch_size - 1) // args.batch_size

            for batch_idx in range(batch_count):
                start = batch_idx * args.batch_size
                end = start + args.batch_size
                batch = clean_payloads[start:end]

                results = await send_batch(
                    client, batch, args.api_url, dry_run=args.dry_run
                )

                for res in results:
                    day_total += 1
                    if "error" in res:
                        day_errors += 1
                    elif res.get("prediction") == 1 or res.get("dry_run"):
                        day_flagged += 1

                # Progress every ~500 transactions
                report_every = max(500 // args.batch_size, 1)
                if (batch_idx + 1) % report_every == 0 or batch_idx == batch_count - 1:
                    print(
                        f"  {day_total:>5}/{len(clean_payloads)}  "
                        f"errors={day_errors}  flagged={day_flagged}",
                        end="\r",
                        flush=True,
                    )

                if args.dry_run:
                    print("\n[dry-run] Stopping after first batch.")
                    return 0

            print(
                f"  {day_total:>5}/{len(clean_payloads)}  "
                f"errors={day_errors}  flagged={day_flagged}   ✓",
                flush=True,
            )
            grand_total += day_total
            grand_errors += day_errors
            grand_flagged += day_flagged

    print(
        f"\n{'=' * 60}\n"
        f"Simulation complete.\n"
        f"  Days simulated  : {total_days}\n"
        f"  Total sent      : {grand_total}\n"
        f"  API errors      : {grand_errors}\n"
        f"  Flagged (pred=1): {grand_flagged}\n"
    )
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Simulate historical transaction API calls using real users/cards from Postgres.\n"
            "Requires POSTGRES_* env vars to be set."
        )
    )
    parser.add_argument(
        "--api-url", default=DEFAULT_API_URL,
        help=f"Fraud-detection scoring endpoint (default: {DEFAULT_API_URL})",
    )
    parser.add_argument(
        "--tx-per-day", type=int, default=DEFAULT_TX_PER_DAY,
        help=f"Transactions per day (default: {DEFAULT_TX_PER_DAY})",
    )
    parser.add_argument(
        "--batch-size", type=int, default=DEFAULT_BATCH_SIZE,
        help=f"Concurrent HTTP requests per batch (default: {DEFAULT_BATCH_SIZE})",
    )
    parser.add_argument(
        "--anomaly-min", type=int, default=DEFAULT_ANOMALY_MIN,
        help=f"Min anomalous transactions per day (default: {DEFAULT_ANOMALY_MIN})",
    )
    parser.add_argument(
        "--anomaly-max", type=int, default=DEFAULT_ANOMALY_MAX,
        help=f"Max anomalous transactions per day (default: {DEFAULT_ANOMALY_MAX})",
    )
    parser.add_argument(
        "--seed", type=int, default=DEFAULT_SEED,
        help=f"Random seed (default: {DEFAULT_SEED})",
    )
    parser.add_argument(
        "--days-from", type=int, default=DEFAULT_DAYS_FROM,
        help=f"Start offset in days ago (default: {DEFAULT_DAYS_FROM})",
    )
    parser.add_argument(
        "--days-to", type=int, default=DEFAULT_DAYS_TO,
        help=f"End offset in days ago inclusive (default: {DEFAULT_DAYS_TO})",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print first batch payload and exit without sending HTTP requests.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.days_from < args.days_to:
        print(
            f"ERROR: --days-from ({args.days_from}) must be >= --days-to ({args.days_to})",
            file=sys.stderr,
        )
        return 1
    if args.anomaly_min > args.anomaly_max:
        print("ERROR: --anomaly-min must be <= --anomaly-max", file=sys.stderr)
        return 1
    return asyncio.run(simulate(args))


if __name__ == "__main__":
    raise SystemExit(main())
