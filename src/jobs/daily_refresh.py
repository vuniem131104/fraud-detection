"""Daily batch job that ages out old transactions and refreshes Redis features.

Scans every per-(user, card) transaction sorted set in Redis, removes
transactions older than a configurable cutoff, and recomputes the associated
feature hash (rolling transaction count, card age, and recency of the last
transaction). Designed to be run as a scheduled command-line job.
"""

from datetime import datetime, timedelta, timezone
from time import perf_counter
import argparse
import asyncio
import json
import os

from redis import asyncio as aioredis
from structlog import get_logger

logger = get_logger(__name__)

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
    """Parse command-line arguments for the refresh job.

    Returns:
        The parsed arguments with ``cutoff_days``, ``concurrency`` and
        ``scan_count`` attributes.
    """
    parser = argparse.ArgumentParser(description="Remove old transactions and refresh feature counts.")
    parser.add_argument("--cutoff-days", type=int, default=30)
    parser.add_argument("--concurrency", type=int, default=25)
    parser.add_argument("--scan-count", type=int, default=100)
    return parser.parse_args()


def features_key_from_transactions_key(transactions_key: str) -> str:
    """Derive the features key matching a given transactions key.

    Args:
        transactions_key: A Redis transactions key.

    Returns:
        The corresponding features key, sharing the same user/card suffix.

    Raises:
        ValueError: If the key does not start with the expected transactions
            prefix.
    """
    if not transactions_key.startswith(TRANSACTIONS_KEY_PREFIX):
        raise ValueError(f"Unexpected transactions key: {transactions_key}")

    return f"{FEATURES_KEY_PREFIX}{transactions_key.removeprefix(TRANSACTIONS_KEY_PREFIX)}"


def local_now() -> datetime:
    """Return the current time in the Ho Chi Minh timezone."""
    return datetime.now(HO_CHI_MINH_TZ)


def to_local_time(value: datetime) -> datetime:
    """Convert a datetime to the Ho Chi Minh timezone.

    Naive datetimes are assumed to already be in local time and are simply
    tagged with the timezone; aware datetimes are converted.

    Args:
        value: The datetime to localize.

    Returns:
        The datetime expressed in the Ho Chi Minh timezone.
    """
    if value.tzinfo is None:
        return value.replace(tzinfo=HO_CHI_MINH_TZ)
    return value.astimezone(HO_CHI_MINH_TZ)


def parse_datetime(value: str) -> datetime:
    """Parse an ISO-8601 timestamp string into a local datetime.

    Accepts a trailing ``Z`` UTC designator and normalizes the result to the
    Ho Chi Minh timezone.

    Args:
        value: An ISO-8601 timestamp string.

    Returns:
        The parsed datetime in the Ho Chi Minh timezone.
    """
    timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return to_local_time(timestamp)


def days_between(start: datetime, end: datetime) -> float:
    """Return the number of days between two datetimes, clamped at zero.

    Args:
        start: The earlier datetime.
        end: The later datetime.

    Returns:
        The elapsed time from ``start`` to ``end`` in days, never negative.
    """
    elapsed_days = (end - start).total_seconds() / SECONDS_PER_DAY
    return max(elapsed_days, 0.0)


async def refresh_one_key(
    redis_client: aioredis.Redis,
    transactions_key: str,
    cutoff_score: int,
    now: datetime,
) -> tuple[int, int]:
    """Refresh a single transactions key and its feature hash.

    Runs a Lua script to atomically drop transactions older than
    ``cutoff_score``, count the survivors, and read the latest transaction and
    the card creation time. It then writes the recomputed features
    (``no_transactions_30_days``, ``card_age_days``, and last-transaction
    recency fields), clearing the recency fields when no transactions remain.

    Args:
        redis_client: The async Redis client.
        transactions_key: The transactions sorted-set key to refresh.
        cutoff_score: The score (epoch seconds) below which transactions are
            removed.
        now: The reference "current" time used for age calculations.

    Returns:
        A tuple of ``(removed_count, remaining_count)``.
    """
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
    """Scan all transaction keys and refresh them with bounded concurrency.

    Iterates over every transactions key via ``SCAN`` and refreshes each one,
    limiting in-flight refreshes to ``concurrency`` tasks and accumulating
    aggregate statistics. If any refresh task fails, pending tasks are
    cancelled and the exception is re-raised.

    Args:
        redis_client: The async Redis client.
        cutoff_days: Age threshold in days beyond which transactions are
            removed.
        concurrency: Maximum number of concurrent key refreshes.
        scan_count: Hint for the number of keys returned per ``SCAN`` batch.

    Returns:
        A stats dictionary with ``keys_scanned``, ``transactions_removed``
        and ``transactions_remaining``.

    Raises:
        ValueError: If ``cutoff_days``, ``concurrency`` or ``scan_count`` is
            less than 1.
    """
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
        """Refresh one key while holding the concurrency semaphore.

        Args:
            transactions_key: The transactions key to refresh.

        Returns:
            A tuple of ``(removed_count, remaining_count)``.
        """
        async with semaphore:
            return await refresh_one_key(redis_client, transactions_key, cutoff_score, now)

    async def collect_done(done: set[asyncio.Task], pending_tasks: set[asyncio.Task]) -> None:
        """Accumulate results from completed tasks into the stats dict.

        If a completed task raised, all still-pending tasks are cancelled and
        awaited before the exception is propagated.

        Args:
            done: The set of completed refresh tasks.
            pending_tasks: The set of tasks still in flight, cancelled on
                error.

        Raises:
            Exception: Re-raises any exception raised by a completed task.
        """
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
    """Run the refresh and log a summary of the work performed.

    Times :func:`refresh_redis_data` and emits a structured log line with the
    number of keys refreshed, transactions removed/remaining, the concurrency
    used and the elapsed time.

    Args:
        redis_client: The async Redis client.
        cutoff_days: Age threshold in days for removing transactions.
        concurrency: Maximum number of concurrent key refreshes.
        scan_count: Hint for the number of keys per ``SCAN`` batch.
    """
    start = perf_counter()

    stats = await refresh_redis_data(
        redis_client,
        cutoff_days=cutoff_days,
        concurrency=concurrency,
        scan_count=scan_count,
    )
    elapsed = perf_counter() - start

    logger.info(
        "Daily Redis refresh completed",
        keys_refreshed=stats["keys_scanned"],
        transactions_removed=stats["transactions_removed"],
        transactions_remaining=stats["transactions_remaining"],
        concurrency=concurrency,
        execution_time_seconds=elapsed,
    )


async def main() -> None:
    """Entry point: build a Redis client from env and run the refresh.

    Reads Redis connection settings from environment variables, creates a
    blocking connection pool, runs the refresh with the parsed CLI arguments
    and always closes the client afterwards.
    """
    args = parse_args()
    redis_pool = aioredis.BlockingConnectionPool(
        host=os.getenv("REDIS_HOST"),
        port=int(os.getenv("REDIS_PORT")),
        db=int(os.getenv("REDIS_DB")),
        decode_responses=True,
        max_connections=int(os.getenv("REDIS_POOL_MAX_CONNECTIONS", "64")),
        timeout=float(os.getenv("REDIS_POOL_TIMEOUT_S", "5")),
    )
    redis_client = aioredis.Redis(connection_pool=redis_pool)

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