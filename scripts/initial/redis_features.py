"""Load recent per-card transaction history and features from Postgres into Redis.

This script reads the last N days (default 30) of transactions from the Postgres
``application`` schema, groups them by ``(user_id, card_id)``, and writes them to
Redis so the fraud-detection service can read them at scoring time:

* each transaction is serialized into a model-feature payload and stored in a
  per-card sorted set (``user:card:transactions:<user_id>_<card_id>``) scored by
  event timestamp, and
* an aggregate feature hash (transaction count, card age, recency, etc.) is
  stored per card under ``user:card:features:<user_id>_<card_id>``.

By default existing matching keys are replaced; ``--append`` keeps them. Redis
connection details and the look-back window are configurable via CLI arguments.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "src"))

from database.postgres import PostgresDatabase


DEFAULT_LOOKBACK_DAYS = 30
DEFAULT_REDIS_HOST = "localhost"
DEFAULT_REDIS_PORT = 6379
DEFAULT_REDIS_DB = 0
SECONDS_PER_DAY = 24 * 60 * 60
HO_CHI_MINH_TZ = timezone(timedelta(hours=7), "Asia/Ho_Chi_Minh")


def redis_transactions_key(user_id: str, card_id: str) -> str:
    """Return the Redis sorted-set key holding a card's transaction history."""
    return f"user:card:transactions:{user_id}_{card_id}"


def redis_features_key(user_id: str, card_id: str) -> str:
    """Return the Redis hash key holding a card's aggregate features."""
    return f"user:card:features:{user_id}_{card_id}"


def local_now() -> datetime:
    """Return the current time in the Ho Chi Minh timezone."""
    return datetime.now(HO_CHI_MINH_TZ)


def to_local_time(value: datetime) -> datetime:
    """Convert a datetime to the Ho Chi Minh timezone.

    Naive datetimes are assumed to already be in Ho Chi Minh local time and are
    simply tagged with that timezone; aware datetimes are converted.
    """
    if value.tzinfo is None:
        return value.replace(tzinfo=HO_CHI_MINH_TZ)
    return value.astimezone(HO_CHI_MINH_TZ)


def iso_local(value: datetime) -> str:
    """Return the ISO-8601 string of ``value`` rendered in Ho Chi Minh local time."""
    return to_local_time(value).isoformat()


def json_default(value: Any) -> Any:
    """JSON serializer hook for datetimes and Decimals.

    Args:
        value: The object ``json`` could not serialize natively.

    Returns:
        An ISO-8601 local string for datetimes, or a float for Decimals.

    Raises:
        TypeError: If ``value`` is of any other unsupported type.
    """
    if isinstance(value, datetime):
        return iso_local(value)
    if isinstance(value, Decimal):
        return float(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


mapping_channel = {
    "web": "W",
    "mobile_app": "C",
    "pos": "R",
}


def days_between(start: datetime, end: datetime) -> float:
    """Return the number of days from ``start`` to ``end`` (clamped at 0, rounded to 4 dp)."""
    elapsed_days = (to_local_time(end) - to_local_time(start)).total_seconds() / SECONDS_PER_DAY
    return round(max(elapsed_days, 0.0), 4)


def issuer_numeric(issuer_code: str) -> int:
    """Extract the digits from an issuer code and return them as an int (0 if none)."""
    digits = "".join(character for character in issuer_code if character.isdigit())
    return int(digits or 0)


def transaction_payload(
    row: dict[str, Any],
    *,
    previous_transaction_count: int,
    previous_created_at: datetime,
) -> dict[str, Any]:
    """Build the model-feature payload for a single stored transaction.

    Flattens a joined transaction/card row into the feature schema expected by the
    scoring service, normalizing email domains, deriving card age (``D4``) and
    recency (``D15``) relative to the previous transaction, and setting the
    transaction sequence count (``C13``).

    Args:
        row: Joined transaction/card row mapping.
        previous_transaction_count: Number of the card's earlier transactions.
        previous_created_at: Timestamp of the card's previous transaction (or the
            card creation time for the first one).

    Returns:
        The feature payload dict for the transaction.
    """
    created_at = to_local_time(row["created_at"])
    card_created_at = to_local_time(row["card_created_at"])
    return {
        "tx_id": str(row["id"]),
        "user_id": str(row["user_id"]),
        "card_id": str(row["card_id"]),
        "issuer_code": issuer_numeric(row["issuer_code"]),
        "card_type": row["card_type"],
        "card_brand": row["card_brand"],
        "card_country": row["card_country"],
        "bin_code": row["card_bin_code"],
        "amount_usd": float(row["amount_usd"]),
        "channel": mapping_channel.get(row["channel"]),
        "billing_zone": row["billing_zone"],
        "billing_country": row["billing_country"],
        "email_purchaser": row["email_purchaser"].split("@")[-1],
        "email_recipient": row["email_recipient"].split("@")[-1],
        "device_info": row["device_info"],
        "device_type": row["device_type"],
        "os_raw": row["os_raw"],
        "browser_raw": row["browser_raw"],
        "screen_resolution": row["screen_resolution"],
        "event_timestamp": iso_local(created_at),
        "C1": random.randint(1, 5),
        "C2": random.randint(1, 5),
        "C13": previous_transaction_count + 1,
        "D4": days_between(card_created_at, created_at),
        "D15": days_between(previous_created_at, created_at),
        "M1": "T",
        "M2": "T",
        "M6": "F",
    }


async def get_data_from_postgres(
    database: PostgresDatabase,
    *,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    now: datetime | None = None,
) -> dict[tuple[str, str], list[dict[str, Any]]]:
    """Fetch recent transactions joined with card data, grouped by user/card.

    Reads all transactions created within ``lookback_days`` of ``now`` (joined to
    their card) and groups them by ``(user_id, card_id)``, newest first within
    each group.

    Args:
        database: Open Postgres database wrapper.
        lookback_days: Size of the look-back window in days.
        now: Reference "now"; defaults to the current local time.

    Returns:
        A mapping of ``(user_id, card_id)`` to its list of transaction row dicts.

    Raises:
        ValueError: If ``lookback_days`` is less than 1.
    """
    if lookback_days < 1:
        raise ValueError("lookback_days must be greater than 0")

    now = now or local_now()
    cutoff = now - timedelta(days=lookback_days)
    grouped_transactions: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)

    async with database.connection() as conn:
        rows = await conn.fetch(
            """
            SELECT
                t.id,
                t.user_id,
                t.card_id,
                t.amount_usd,
                t.channel,
                t.billing_zone,
                t.billing_country,
                t.email_purchaser,
                t.email_recipient,
                t.device_info,
                t.device_type,
                t.os_raw,
                t.browser_raw,
                t.screen_resolution,
                t.created_at,
                c.issuer_code,
                c.type AS card_type,
                c.brand AS card_brand,
                c.bin_code AS card_bin_code,
                c.country AS card_country,
                c.created_at AS card_created_at
            FROM application.transactions AS t
            JOIN application.cards AS c
                ON c.id = t.card_id
               AND c.user_id = t.user_id
            WHERE t.created_at >= $1
              AND t.card_id IS NOT NULL
            ORDER BY t.user_id, t.card_id, t.created_at DESC
            """,
            cutoff,
        )

    for row in rows:
        grouped_transactions[(row["user_id"], row["card_id"])].append(dict(row))

    return dict(grouped_transactions)


def feature_payload(
    rows: list[dict[str, Any]],
    *,
    now: datetime,
) -> dict[str, Any]:
    """Build the aggregate feature hash for one card's recent transactions.

    Args:
        rows: A card's transaction rows, ordered newest-first (``rows[0]`` is the
            most recent transaction).
        now: Reference "now" used to compute card age and recency.

    Returns:
        A dict with the transaction count, card age in days, days since the last
        transaction, and the card-created/last-transaction timestamps.
    """
    newest = rows[0]
    now = to_local_time(now)
    card_created_at = to_local_time(newest["card_created_at"])
    last_txn_at = to_local_time(newest["created_at"])
    return {
        "no_transactions_30_days": int(len(rows)),
        "card_age_days": float((now - card_created_at).total_seconds() / SECONDS_PER_DAY),
        "no_days_since_last_txn": float((now - last_txn_at).total_seconds() / SECONDS_PER_DAY),
        "card_created_at": iso_local(card_created_at),
        "last_txn_at": iso_local(last_txn_at),
    }


async def store_grouped_transactions(
    grouped_transactions: dict[tuple[str, str], list[dict[str, Any]]],
    *,
    host: str = DEFAULT_REDIS_HOST,
    port: int = DEFAULT_REDIS_PORT,
    db: int = DEFAULT_REDIS_DB,
    now: datetime | None = None,
    clear_existing: bool = True,
) -> int:
    """Write grouped transactions and aggregate features into Redis.

    For each ``(user_id, card_id)`` group, optionally clears existing keys, then
    pipelines each transaction into the per-card sorted set (scored by event
    timestamp) and writes the aggregate feature hash. Per-transaction recency is
    computed from the chronological ordering within the group.

    Args:
        grouped_transactions: Mapping of ``(user_id, card_id)`` to transaction rows.
        host: Redis host.
        port: Redis port.
        db: Redis database number.
        now: Reference "now" used for feature computation; defaults to local now.
        clear_existing: If true, delete matching keys before writing (replace mode);
            otherwise append to existing keys.

    Returns:
        The total number of transactions stored across all groups.

    Raises:
        RuntimeError: If the ``redis`` Python package is not installed.
    """
    try:
        from redis import asyncio as aioredis
    except ModuleNotFoundError as exc:
        if exc.name == "redis":
            raise RuntimeError("Missing dependency: install the redis Python package") from exc
        raise

    now = now or local_now()
    redis_client = aioredis.Redis(host=host, port=port, db=db, decode_responses=True)
    stored_count = 0

    try:
        pipeline = redis_client.pipeline(transaction=False)
        for (user_id, card_id), rows in grouped_transactions.items():
            if not rows:
                continue

            transactions_key = redis_transactions_key(user_id, card_id)
            features_key = redis_features_key(user_id, card_id)
            if clear_existing:
                pipeline.delete(transactions_key, features_key)

            chronological_rows = sorted(rows, key=lambda row: row["created_at"])
            previous_created_by_id: dict[str, datetime] = {}
            previous_count_by_id: dict[str, int] = {}
            previous_created_at = to_local_time(chronological_rows[0]["card_created_at"])
            for previous_transaction_count, row in enumerate(chronological_rows):
                previous_created_by_id[row["id"]] = previous_created_at
                previous_count_by_id[row["id"]] = previous_transaction_count
                previous_created_at = to_local_time(row["created_at"])

            for row in rows:
                created_at = to_local_time(row["created_at"])
                pipeline.zadd(
                    transactions_key,
                    {
                        json.dumps(
                            transaction_payload(
                                row,
                                previous_transaction_count=previous_count_by_id[row["id"]],
                                previous_created_at=previous_created_by_id[row["id"]],
                            ),
                            default=json_default,
                            separators=(",", ":"),
                        ): int(created_at.timestamp())
                    },
                )

            pipeline.hset(
                features_key,
                mapping=feature_payload(rows, now=now),
            )
            stored_count += len(rows)

        await pipeline.execute()
    finally:
        await redis_client.aclose()

    return stored_count


def build_parser() -> argparse.ArgumentParser:
    """Build the argument parser for the Redis feature-loading CLI."""
    parser = argparse.ArgumentParser(
        description="Load last-30-day card transaction features from Postgres into Redis."
    )
    parser.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS)
    parser.add_argument("--redis-host", default=DEFAULT_REDIS_HOST)
    parser.add_argument("--redis-port", type=int, default=DEFAULT_REDIS_PORT)
    parser.add_argument("--redis-db", type=int, default=DEFAULT_REDIS_DB)
    parser.add_argument(
        "--append",
        action="store_true",
        help="Append to existing Redis sorted sets instead of replacing matching keys.",
    )
    return parser


async def main() -> None:
    """CLI entry point: load recent transactions from Postgres into Redis.

    Parses arguments, fetches the grouped transactions for the configured
    look-back window, writes them and their features to Redis, and prints a
    summary of how much was loaded.
    """
    args = build_parser().parse_args()
    now = local_now()
    database = PostgresDatabase.from_env()
    await database.open()
    try:
        grouped_transactions = await get_data_from_postgres(
            database,
            lookback_days=args.lookback_days,
            now=now,
        )
        stored_count = await store_grouped_transactions(
            grouped_transactions,
            host=args.redis_host,
            port=args.redis_port,
            db=args.redis_db,
            now=now,
            clear_existing=not args.append,
        )
    finally:
        await database.close()
    print(
        "Loaded "
        f"{stored_count} transactions across {len(grouped_transactions)} user/card groups."
    )


if __name__ == "__main__":
    asyncio.run(main())
