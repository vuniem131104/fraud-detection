"""Async PostgreSQL access layer backed by an ``asyncpg`` connection pool.

Defines :class:`PostgresDatabase`, a thin wrapper that manages the lifecycle
of an ``asyncpg`` pool and exposes context managers for acquiring connections
and running transactions, plus a convenience ``execute`` helper.
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import AsyncIterator, Sequence

import asyncpg


class PostgresDatabase:
    """Manages an ``asyncpg`` connection pool to a PostgreSQL database.

    Holds connection settings and a lazily created pool, and provides async
    context managers for borrowing connections and running transactions.
    """

    def __init__(
        self,
        port: int,
        user: str,
        password: str,
        host: str,
        database: str,
        min_pool_size: int = 1,
        max_pool_size: int = 10,
        timeout_s: float = 5.0,
    ) -> None:
        """Store connection parameters without opening the pool.

        Args:
            port: PostgreSQL server port.
            user: Database user name.
            password: Database password.
            host: Database host.
            database: Database name.
            min_pool_size: Minimum number of pooled connections.
            max_pool_size: Maximum number of pooled connections.
            timeout_s: Connection acquisition timeout in seconds.
        """
        self.port = port
        self.user = user
        self.password = password
        self.host = host
        self.database = database
        self.min_pool_size = min_pool_size
        self.max_pool_size = max_pool_size
        self.timeout_s = timeout_s
        self._pool: asyncpg.Pool | None = None

    @classmethod
    def from_env(cls) -> PostgresDatabase:
        """Build a :class:`PostgresDatabase` from environment variables.

        Reads the required ``POSTGRES_*`` connection variables and the
        optional pool sizing/timeout variables.

        Returns:
            A configured (but not yet opened) :class:`PostgresDatabase`.

        Raises:
            ValueError: If any required Postgres environment variable is
                missing or empty.
        """
        variable_names = (
            "POSTGRES_PORT",
            "POSTGRES_USER",
            "POSTGRES_PASSWORD",
            "POSTGRES_HOST",
            "POSTGRES_DB",
        )
        values = {name: os.getenv(name, "") for name in variable_names}
        missing = [name for name, value in values.items() if not value]
        if missing:
            raise ValueError(f"Missing Postgres environment variables: {', '.join(missing)}")
        
        return cls(
            port=int(values["POSTGRES_PORT"]),
            user=values["POSTGRES_USER"],
            password=values["POSTGRES_PASSWORD"],
            host=values["POSTGRES_HOST"],
            database=values["POSTGRES_DB"],
            min_pool_size=int(os.getenv("POSTGRES_POOL_MIN_SIZE", "1")),
            max_pool_size=int(os.getenv("POSTGRES_POOL_MAX_SIZE", "10")),
            timeout_s=float(os.getenv("POSTGRES_POOL_TIMEOUT_S", "5")),
        )

    @property
    def pool(self) -> asyncpg.Pool:
        """Return the active connection pool.

        Returns:
            The underlying ``asyncpg`` pool.

        Raises:
            RuntimeError: If the database has not been opened yet.
        """
        if self._pool is None:
            raise RuntimeError("PostgresDatabase has not been opened")
        return self._pool

    async def open(self) -> None:
        """Create the connection pool if it does not already exist.

        Idempotent: calling it when the pool is already open is a no-op.
        """
        if self._pool is not None:
            return

        self._pool = await asyncpg.create_pool(
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database,
            min_size=self.min_pool_size,
            max_size=self.max_pool_size,
            timeout=self.timeout_s,
        )

    async def close(self) -> None:
        """Close the connection pool if it is open.

        Idempotent: calling it when no pool exists is a no-op.
        """
        if self._pool is None:
            return
        await self._pool.close()
        self._pool = None

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[asyncpg.Connection]:
        """Acquire a pooled connection for the duration of the context.

        Yields:
            A connection borrowed from the pool, returned automatically on
            exit.
        """
        async with self.pool.acquire() as conn:
            yield conn

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[asyncpg.Connection]:
        """Acquire a connection and run the context inside a transaction.

        The transaction is committed on normal exit and rolled back if the
        body raises.

        Yields:
            A connection bound to an open transaction.
        """
        async with self.connection() as conn:
            async with conn.transaction():
                yield conn

    async def execute(self, query: str, params: Sequence[object] | None = None) -> str:
        """Execute a query on a pooled connection and return its status.

        Args:
            query: The SQL statement to execute.
            params: Optional positional query parameters.

        Returns:
            The command status string returned by ``asyncpg``.
        """
        async with self.connection() as conn:
            return await conn.execute(query, *(params or ()))
