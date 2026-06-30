"""Repository: fetch TransactionAmt data from the PostgreSQL transactions table."""

from __future__ import annotations

import asyncpg

_QUERY_LAST_7_DAYS = """
    SELECT amount_usd::double precision
    FROM   application.transactions
    WHERE  created_at >= NOW() - INTERVAL '7 days'
      AND  amount_usd IS NOT NULL
    ORDER  BY created_at DESC
"""


async def fetch_amounts_last_7_days(conn: asyncpg.Connection) -> list[float]:
    """Return all non-null ``amount_usd`` values from the last 7 days.

    Args:
        conn: An active asyncpg connection (borrowed from the pool).

    Returns:
        A list of floats, one per qualifying row.  May be empty if no
        transactions were recorded in the window.
    """
    rows = await conn.fetch(_QUERY_LAST_7_DAYS)
    return [row["amount_usd"] for row in rows]
