"""One-time (re-runnable) bootstrap of Redis velocity state from Postgres.

Doc §6.3: replay `application.transaction_features` into the same Redis keys
`predict.py`'s Lua scripts read/write (`card:transactions`, `card:aggregate`,
`card:declines`, `user:transactions`, `user:aggregate`), so on-demand serving
starts with correct state at cutover instead of every card/user cold-starting.

Idempotent: aggregate hashes are HSET with values recomputed fresh from
Postgres (not HINCRBY), and ZADD on the same member+score is a no-op — safe
to rerun any time (e.g. after a Redis flush incident).

Run:
    python scripts/bootstrap_velocity_state.py
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path

import asyncpg
from redis import asyncio as aioredis

REPO_ROOT = Path(__file__).resolve().parent.parent

RETENTION_S = 25 * 3600  # phải khớp _VELOCITY_RETENTION_S trong predict.py
AGG_TTL_S = 90 * 86400   # phải khớp _VELOCITY_AGG_TTL_S trong predict.py
PIPELINE_BATCH = 500


def _load_env() -> None:
    env_path = REPO_ROOT / ".env"
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip())


async def main() -> None:
    _load_env()

    pg = await asyncpg.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        database=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    try:
        window_rows = await pg.fetch(
            """
            SELECT transaction_id, user_id, card_id, amount_usd,
                   extract(epoch FROM created_at) AS epoch,
                   is_declined
            FROM application.transaction_features
            WHERE created_at >= now() - interval '25 hours'
            """
        )
        card_agg_rows = await pg.fetch(
            """
            SELECT card_id,
                   count(*) AS cnt,
                   sum(amount_usd) AS sum_amt,
                   sum(amount_usd * amount_usd) AS sum_sq,
                   extract(epoch FROM max(created_at)) AS last_ts
            FROM application.transaction_features
            GROUP BY card_id
            """
        )
        user_agg_rows = await pg.fetch(
            """
            SELECT user_id, extract(epoch FROM max(created_at)) AS last_ts
            FROM application.transaction_features
            GROUP BY user_id
            """
        )
    finally:
        await pg.close()

    print(
        f"postgres: {len(window_rows)} tx in last 25h, "
        f"{len(card_agg_rows)} cards, {len(user_agg_rows)} users (lifetime)"
    )

    card_tx: dict[str, dict[str, float]] = {}
    card_declines: dict[str, dict[str, float]] = {}
    user_tx: dict[str, dict[str, float]] = {}
    for row in window_rows:
        epoch = float(row["epoch"])  # extract(epoch FROM ...) -> numeric -> Decimal via asyncpg
        member = f"{row['transaction_id']}|{row['amount_usd']}"
        card_tx.setdefault(row["card_id"], {})[member] = epoch
        user_tx.setdefault(row["user_id"], {})[member] = epoch
        if row["is_declined"]:
            card_declines.setdefault(row["card_id"], {})[row["transaction_id"]] = epoch

    card_tx_prefix = os.environ["CARD_TRANSACTIONS_KEY"]
    card_agg_prefix = os.environ["CARD_AGGREGATE_KEY"]
    card_declines_prefix = os.environ["CARD_DECLINES_KEY"]
    user_tx_prefix = os.environ["USER_TRANSACTIONS_KEY"]
    user_agg_prefix = os.environ["USER_AGGREGATE_KEY"]

    redis_client = aioredis.Redis(
        host=os.environ["REDIS_HOST"],
        port=int(os.environ["REDIS_PORT"]),
        db=int(os.getenv("REDIS_DB", "0")),
        decode_responses=True,
    )
    pipe = redis_client.pipeline(transaction=False)
    pending = 0

    async def flush() -> None:
        nonlocal pending
        if pending:
            await pipe.execute()
            pending = 0

    for card_id, members in card_tx.items():
        key = f"{card_tx_prefix}:{card_id}"
        pipe.zadd(key, members)
        pipe.expire(key, RETENTION_S)
        pending += 2
        if pending >= PIPELINE_BATCH:
            await flush()

    for card_id, members in card_declines.items():
        key = f"{card_declines_prefix}:{card_id}"
        pipe.zadd(key, members)
        pipe.expire(key, RETENTION_S)
        pending += 2
        if pending >= PIPELINE_BATCH:
            await flush()

    for user_id, members in user_tx.items():
        key = f"{user_tx_prefix}:{user_id}"
        pipe.zadd(key, members)
        pipe.expire(key, RETENTION_S)
        pending += 2
        if pending >= PIPELINE_BATCH:
            await flush()

    for row in card_agg_rows:
        key = f"{card_agg_prefix}:{row['card_id']}"
        pipe.hset(key, mapping={
            "count_so_far": int(row["cnt"]),
            "sum_so_far": float(row["sum_amt"] or 0.0),
            "sum_square": float(row["sum_sq"] or 0.0),
            "last_txn_at": f"{float(row['last_ts']):.6f}",
        })
        pipe.expire(key, AGG_TTL_S)
        pending += 2
        if pending >= PIPELINE_BATCH:
            await flush()

    for row in user_agg_rows:
        key = f"{user_agg_prefix}:{row['user_id']}"
        pipe.hset(key, mapping={"last_txn_at": f"{float(row['last_ts']):.6f}"})
        pipe.expire(key, AGG_TTL_S)
        pending += 2
        if pending >= PIPELINE_BATCH:
            await flush()

    await flush()
    await redis_client.aclose()

    print(
        f"redis: {len(card_tx)} card windows, {len(card_declines)} card-decline sets, "
        f"{len(user_tx)} user windows, {len(card_agg_rows)} card aggregates, "
        f"{len(user_agg_rows)} user aggregates"
    )


if __name__ == "__main__":
    asyncio.run(main())
