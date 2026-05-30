from __future__ import annotations

import argparse
import asyncio
import json
import random
import sys
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "src"))

from database.postgres import PostgresDatabase

load_dotenv()

DEFAULT_LOOKBACK_DAYS = 30
DEFAULT_REDIS_HOST = "localhost"
DEFAULT_REDIS_PORT = 6379
DEFAULT_REDIS_DB = 0
SECONDS_PER_DAY = 24 * 60 * 60


def redis_transactions_key(user_id: int, card_id: int) -> str:
    return f"user:card:transactions:{user_id}_{card_id}"


def redis_features_key(user_id: int, card_id: int) -> str:
    return f"user:card:features:{user_id}_{card_id}"


def utc_now() -> datetime:
    return datetime.now(UTC)


def to_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def iso_z(value: datetime) -> str:
    return to_utc(value).isoformat().replace("+00:00", "Z")


def json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return iso_z(value)
    if isinstance(value, Decimal):
        return float(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


mapping_channel = {
    "web": "W",
    "mobile_app": "C",
    "pos": "R",
}


def days_between(start: datetime, end: datetime) -> float:
    elapsed_days = (to_utc(end) - to_utc(start)).total_seconds() / SECONDS_PER_DAY
    return round(max(elapsed_days, 0.0), 4)


def issuer_numeric(issuer_code: str) -> int:
    digits = "".join(character for character in issuer_code if character.isdigit())
    return int(digits or 0)


def transaction_payload(
    row: dict[str, Any],
    *,
    previous_transaction_count: int,
    previous_created_at: datetime,
) -> dict[str, Any]:
    created_at = to_utc(row["created_at"])
    card_created_at = to_utc(row["card_created_at"])
    return {
        "transaction_id": str(row["id"]),
        "user_id": str(row["user_id"]),
        "card_id": str(row["card_id"]),
        "issuer_code": issuer_numeric(row["issuer_code"]),
        "card_type": row["card_type"],
        "card_brand": row["card_brand"],
        "card_country": row["card_country"],
        "amount": float(row["amount_usd"]),
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
        "created_at": iso_z(created_at),
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
) -> dict[tuple[int, int], list[dict[str, Any]]]:
    if lookback_days < 1:
        raise ValueError("lookback_days must be greater than 0")

    now = now or utc_now()
    cutoff = now - timedelta(days=lookback_days)
    grouped_transactions: dict[tuple[int, int], list[dict[str, Any]]] = defaultdict(list)

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
    newest = rows[0]
    card_created_at = to_utc(newest["card_created_at"])
    last_txn_at = to_utc(newest["created_at"])
    return {
        "issuer_code": issuer_numeric(newest["issuer_code"]),
        "card_type": newest["card_type"],
        "card_brand": newest["card_brand"],
        "card_country": newest["card_country"],
        "no_transactions_30_days": len(rows),
        "card_ages_days": (now - card_created_at).total_seconds() / SECONDS_PER_DAY,
        "no_days_since_last_txn": (now - last_txn_at).total_seconds() / SECONDS_PER_DAY,
        "card_created_at": iso_z(card_created_at),
        "last_txn_at": iso_z(last_txn_at),
    }


async def store_grouped_transactions(
    grouped_transactions: dict[tuple[int, int], list[dict[str, Any]]],
    *,
    host: str = DEFAULT_REDIS_HOST,
    port: int = DEFAULT_REDIS_PORT,
    db: int = DEFAULT_REDIS_DB,
    now: datetime | None = None,
    clear_existing: bool = True,
) -> int:
    try:
        from redis import asyncio as aioredis
    except ModuleNotFoundError as exc:
        if exc.name == "redis":
            raise RuntimeError("Missing dependency: install the redis Python package") from exc
        raise

    now = now or utc_now()
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
            previous_created_by_id: dict[int, datetime] = {}
            previous_count_by_id: dict[int, int] = {}
            previous_created_at = to_utc(chronological_rows[0]["card_created_at"])
            for previous_transaction_count, row in enumerate(chronological_rows):
                previous_created_by_id[row["id"]] = previous_created_at
                previous_count_by_id[row["id"]] = previous_transaction_count
                previous_created_at = to_utc(row["created_at"])

            for row in rows:
                created_at = to_utc(row["created_at"])
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
    args = build_parser().parse_args()
    now = utc_now()
    database = PostgresDatabase()
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
