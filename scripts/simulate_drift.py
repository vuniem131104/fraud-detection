"""Simulate (and revert) data drift on ``amount_usd`` for the drift-detection service.

Inserts synthetic transactions whose amounts follow the same lognormal shape as the
fake-data generator (``lognormvariate(mu, 0.6)``) but with the centre shifted by
``ln(multiplier)`` — i.e. every simulated user suddenly spends ``multiplier``× their
usual amount. The drift service reads ``application.transactions.amount_usd`` over the
last 30 days, so pushing enough shifted rows into that window moves the log-scale
Wasserstein distance past its 0.1 threshold.

Simulated rows are tagged with the id prefix ``dddddddd`` (still a valid 32-hex id),
so ``--cleanup`` can remove them all and return the DB to its previous state. Existing
users/cards/merchants/devices are reused — no new entities, all FKs intact. The rows
bypass the scoring pipeline entirely (no Kafka, no prediction_logs), so the A/B
dashboard is unaffected.

Usage:
    python scripts/simulate_drift.py                       # inject drift (25k rows, x4)
    python scripts/simulate_drift.py --rows 10000 --multiplier 6
    python scripts/simulate_drift.py --cleanup             # revert everything
"""

from __future__ import annotations

import argparse
import math
import os
import random
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from drift_detection.detector import _compute_wasserstein, _wd_label  # noqa: E402

MARKER = "dddddddd"  # id prefix of simulated rows — matches the ^[0-9a-f]{32}$ check
BASELINE_PARQUET = REPO_ROOT / "dataset" / "training_data.parquet"
LOGNORMAL_SIGMA = 0.6  # same shape as generate_fake_data.legit_amount
AMOUNT_CAP = 9000.0    # same cap as the generator

# Same query as drift_detection.repository — what the service will actually see.
CURRENT_WINDOW_SQL = """
    SELECT amount_usd::double precision
    FROM   application.transactions
    WHERE  created_at >= NOW() - INTERVAL '30 days'
      AND  amount_usd IS NOT NULL
"""


def load_env(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def connect() -> psycopg.Connection:
    load_env(REPO_ROOT / ".env")
    return psycopg.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ["POSTGRES_PORT"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
    )


def report(conn: psycopg.Connection) -> None:
    """Recompute the exact number the drift service will return on its next /detect."""
    with conn.cursor() as cur:
        cur.execute(CURRENT_WINDOW_SQL)
        current = np.array([r[0] for r in cur.fetchall()], dtype=float)
    baseline = (
        pd.read_parquet(BASELINE_PARQUET, columns=["amount_usd"])["amount_usd"]
        .dropna()
        .to_numpy(dtype=float)
    )
    wd = _compute_wasserstein(baseline, current)
    print(
        f"→ next /detect will see: wasserstein={wd} ({_wd_label(wd)}), "
        f"n_current={len(current):,}, current_mean={current.mean():.2f}, "
        f"baseline_mean={baseline.mean():.2f}"
    )


def cleanup(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM application.transactions WHERE id LIKE %s", (f"{MARKER}%",)
        )
        print(f"Deleted {cur.rowcount:,} simulated transactions")
    conn.commit()


def simulate(conn: psycopg.Connection, rows: int, multiplier: float, hours: float, seed: int) -> None:
    rng = random.Random(seed)
    now = datetime.now(timezone.utc)

    with conn.cursor() as cur:
        # Reuse real entities; only cards older than the injection window so
        # created_at never precedes card.created_at.
        cur.execute(
            """
            SELECT c.id, c.user_id, u.country_code, u.email
            FROM application.cards c
            JOIN application.users u ON u.id = c.user_id
            WHERE c.created_at < NOW() - (%s * interval '1 hour')
            ORDER BY random() LIMIT 2000
            """,
            (hours,),
        )
        pairs = cur.fetchall()
        cur.execute("SELECT id FROM application.merchants ORDER BY random() LIMIT 500")
        merchants = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT id FROM application.devices ORDER BY random() LIMIT 500")
        devices = [r[0] for r in cur.fetchall()]
        # Centre the drifted lognormal on the CURRENT typical amount, shifted by ln(multiplier).
        cur.execute(
            "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY amount_usd) "
            "FROM application.transactions WHERE created_at >= NOW() - INTERVAL '30 days'"
        )
        median = float(cur.fetchone()[0] or 40.0)

    if not pairs or not merchants or not devices:
        raise SystemExit("Not enough existing entities to sample from — aborting.")

    mu = math.log(median) + math.log(multiplier)
    print(
        f"Injecting {rows:,} rows over the last {hours:g}h: "
        f"amount ~ lognormal(ln({median:.2f}) + ln({multiplier:g}), {LOGNORMAL_SIGMA}) "
        f"→ median ≈ ${median * multiplier:,.2f}"
    )

    batch = []
    for _ in range(rows):
        card_id, user_id, country, email = rng.choice(pairs)
        amount = min(max(rng.lognormvariate(mu, LOGNORMAL_SIGMA), 0.5), AMOUNT_CAP)
        created = now - timedelta(seconds=rng.uniform(0, hours * 3600))
        batch.append(
            (
                f"{MARKER}{rng.getrandbits(96):024x}",
                user_id,
                card_id,
                rng.choice(merchants),
                rng.choice(devices),
                Decimal(f"{amount:.2f}"),
                "USD",
                rng.choice(["web", "mobile_app", "pos"]),
                country,
                country,
                email,
                email,
                "declined" if rng.random() < 0.055 else "approved",
                created,
            )
        )

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO application.transactions (
                id, user_id, card_id, merchant_id, device_id,
                amount_usd, currency, channel,
                billing_country_code, ip_country_code,
                email_purchaser, email_recipient, status, created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            batch,
        )
    conn.commit()
    print(f"Inserted {rows:,} drifted transactions (id prefix '{MARKER}')")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--rows", type=int, default=25000, help="number of drifted transactions to insert")
    parser.add_argument("--multiplier", type=float, default=4.0, help="amount scale factor (mu shift = ln(multiplier))")
    parser.add_argument("--hours", type=float, default=24.0, help="spread created_at over the last N hours")
    parser.add_argument("--seed", type=int, default=42, help="RNG seed for reproducibility")
    parser.add_argument("--cleanup", action="store_true", help="delete all simulated rows instead of inserting")
    args = parser.parse_args()

    with connect() as conn:
        if args.cleanup:
            cleanup(conn)
        else:
            simulate(conn, args.rows, args.multiplier, args.hours, args.seed)
        report(conn)


if __name__ == "__main__":
    main()
