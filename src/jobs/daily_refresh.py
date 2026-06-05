from redis import asyncio as aioredis
from datetime import datetime, timedelta, timezone
from time import perf_counter
import argparse
import asyncio
import json


TRANSACTIONS_KEY_PREFIX = "user:card:transactions:"
FEATURES_KEY_PREFIX = "user:card:features:"
TRANSACTIONS_KEY_PATTERN = f"{TRANSACTIONS_KEY_PREFIX}*"
SECONDS_PER_DAY = 24 * 60 * 60
HO_CHI_MINH_TZ = timezone(timedelta(hours=7), "Asia/Ho_Chi_Minh")

refresh_key_script = """
local removed = redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, ARGV[1])
local remaining = redis.call('ZCARD', KEYS[1])
local latest = redis.call('ZREVRANGE', KEYS[1], 0, 0)[1] or false
local card_created_at = redis.call('HGET', KEYS[2], 'card_created_at') or false
return {removed, remaining, latest, card_created_at}
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Remove old transactions and refresh feature counts.")
    parser.add_argument("--cutoff-days", type=int, default=30)
    parser.add_argument("--concurrency", type=int, default=25)
    parser.add_argument("--scan-count", type=int, default=100)
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--db", type=int, default=0)
    return parser.parse_args()


def features_key_from_transactions_key(transactions_key: str) -> str:
    if not transactions_key.startswith(TRANSACTIONS_KEY_PREFIX):
        raise ValueError(f"Unexpected transactions key: {transactions_key}")

    return f"{FEATURES_KEY_PREFIX}{transactions_key.removeprefix(TRANSACTIONS_KEY_PREFIX)}"


def local_now() -> datetime:
    return datetime.now(HO_CHI_MINH_TZ)


def to_local_time(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=HO_CHI_MINH_TZ)
    return value.astimezone(HO_CHI_MINH_TZ)


def parse_datetime(value: str) -> datetime:
    timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return to_local_time(timestamp)


def days_between(start: datetime, end: datetime) -> float:
    elapsed_days = (end - start).total_seconds() / SECONDS_PER_DAY
    return max(elapsed_days, 0.0)


async def refresh_one_key(
    redis_client: aioredis.Redis,
    transactions_key: str,
    cutoff_score: int,
    now: datetime,
) -> tuple[int, int]:
    features_key = features_key_from_transactions_key(transactions_key)
    refresh_result = await redis_client.eval(
        refresh_key_script,
        2,
        transactions_key,
        features_key,
        cutoff_score,
    )
    removed_count, remaining_count, latest_transaction_raw, card_created_at = (
        list(refresh_result) + [None, None]
    )[:4]
    feature_updates = {
        "no_transactions_30_days": remaining_count,
    }

    if card_created_at:
        feature_updates["card_age_days"] = days_between(parse_datetime(card_created_at), now)

    if latest_transaction_raw:
        latest_transaction = json.loads(latest_transaction_raw)
        last_txn_at = latest_transaction["event_timestamp"]
        feature_updates["last_txn_at"] = last_txn_at
        feature_updates["no_days_since_last_txn"] = days_between(parse_datetime(last_txn_at), now)
        await redis_client.hset(features_key, mapping=feature_updates)
    else:
        pipeline = redis_client.pipeline(transaction=False)
        pipeline.hset(features_key, mapping=feature_updates)
        pipeline.hdel(features_key, "last_txn_at", "no_days_since_last_txn")
        await pipeline.execute()

    return removed_count, remaining_count


async def refresh_redis_data(
    redis_client: aioredis.Redis,
    *,
    cutoff_days: int = 30,
    concurrency: int = 25,
    scan_count: int = 100,
) -> dict:
    if cutoff_days < 1:
        raise ValueError("cutoff_days must be greater than 0")
    if concurrency < 1:
        raise ValueError("concurrency must be greater than 0")
    if scan_count < 1:
        raise ValueError("scan_count must be greater than 0")

    now = local_now()
    cutoff = now - timedelta(days=cutoff_days)
    cutoff_score = int(cutoff.timestamp())
    semaphore = asyncio.Semaphore(concurrency)
    stats = {
        "keys_scanned": 0,
        "transactions_removed": 0,
        "transactions_remaining": 0,
    }

    async def refresh_with_limit(transactions_key: str) -> tuple[int, int]:
        async with semaphore:
            return await refresh_one_key(redis_client, transactions_key, cutoff_score, now)

    async def collect_done(done: set[asyncio.Task], pending_tasks: set[asyncio.Task]) -> None:
        for task in done:
            try:
                removed_count, remaining_count = task.result()
            except Exception:
                for pending_task in pending_tasks:
                    pending_task.cancel()
                if pending_tasks:
                    await asyncio.gather(*pending_tasks, return_exceptions=True)
                raise

            stats["transactions_removed"] += removed_count
            stats["transactions_remaining"] += remaining_count

    pending = set()
    async for transactions_key in redis_client.scan_iter(
        match=TRANSACTIONS_KEY_PATTERN,
        count=scan_count,
    ):
        stats["keys_scanned"] += 1
        pending.add(asyncio.create_task(refresh_with_limit(transactions_key)))

        if len(pending) >= concurrency:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            await collect_done(done, pending)

    if pending:
        done, _ = await asyncio.wait(pending)
        await collect_done(done, set())

    return stats


async def run_refresh(
    redis_client: aioredis.Redis,
    *,
    cutoff_days: int,
    concurrency: int,
    scan_count: int,
) -> None:
    start = perf_counter()

    stats = await refresh_redis_data(
        redis_client,
        cutoff_days=cutoff_days,
        concurrency=concurrency,
        scan_count=scan_count,
    )
    elapsed = perf_counter() - start

    print(f"Keys refreshed: {stats['keys_scanned']}")
    print(f"Transactions removed: {stats['transactions_removed']}")
    print(f"Transactions remaining: {stats['transactions_remaining']}")
    print(f"Concurrency: {concurrency}")
    print(f"Execution time: {elapsed:.6f} seconds")


async def main() -> None:
    args = parse_args()
    redis_client = aioredis.Redis(
        host=args.host,
        port=args.port,
        db=args.db,
        decode_responses=True,
    )

    try:
        await run_refresh(
            redis_client,
            cutoff_days=args.cutoff_days,
            concurrency=args.concurrency,
            scan_count=args.scan_count,
        )
    finally:
        await redis_client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
