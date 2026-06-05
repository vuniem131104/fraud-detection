from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import AsyncIterator, Sequence

import asyncpg


class PostgresDatabase:
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
        if self._pool is None:
            raise RuntimeError("PostgresDatabase has not been opened")
        return self._pool

    async def open(self) -> None:
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
        if self._pool is None:
            return
        await self._pool.close()
        self._pool = None

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[asyncpg.Connection]:
        async with self.pool.acquire() as conn:
            yield conn

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[asyncpg.Connection]:
        async with self.connection() as conn:
            async with conn.transaction():
                yield conn

    async def execute(self, query: str, params: Sequence[object] | None = None) -> str:
        async with self.connection() as conn:
            return await conn.execute(query, *(params or ()))
