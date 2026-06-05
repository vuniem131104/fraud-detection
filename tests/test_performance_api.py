#!/usr/bin/env python3
"""Locust load test for the fraud scoring API.

Run a 50 RPS test with:

    uv run locust -f tests/test_performance_api.py \
        --host http://localhost:1311 \
        --headless --users 50 --spawn-rate 50 --run-time 10s \
        --stop-timeout 30s --html report.html --csv report

Each user is capped at one task per second. Locust cannot maintain the target
throughput when API latency exceeds the task interval, so increase --users when
testing an overloaded service.
"""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import asyncpg
from gevent.lock import Semaphore
from locust import HttpUser, constant_throughput, events, task
from locust.runners import MasterRunner


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENTITY_LIMIT = int(os.getenv("LOCUST_ENTITY_LIMIT", "500"))
RPS_PER_USER = float(os.getenv("LOCUST_RPS_PER_USER", "1"))
HO_CHI_MINH_TZ = timezone(timedelta(hours=7), "Asia/Ho_Chi_Minh")

_entities: list[tuple[str, str]] = []
_entity_index = 0
_entity_lock = Semaphore()


def load_dotenv() -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in (PROJECT_ROOT / ".env").read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, value = line.split("=", 1)
        values[name.strip()] = value.strip().strip("\"'")
    return values


async def query_valid_entities() -> list[tuple[str, str]]:
    dotenv = load_dotenv()

    def env(name: str) -> str:
        value = os.getenv(name) or dotenv.get(name)
        if not value:
            raise RuntimeError(f"Missing {name} in the environment or .env")
        return value

    connection = await asyncpg.connect(
        host=env("POSTGRES_HOST"),
        port=int(env("POSTGRES_PORT")),
        user=env("POSTGRES_USER"),
        password=env("POSTGRES_PASSWORD"),
        database=env("POSTGRES_DB"),
    )
    try:
        rows = await connection.fetch(
            """
            SELECT u.id AS user_id, c.id AS card_id
            FROM application.users AS u
            JOIN application.cards AS c ON c.user_id = u.id
            LEFT JOIN application.transactions AS t
                ON t.user_id = u.id
               AND t.card_id = c.id
            GROUP BY u.id, c.id
            ORDER BY count(t.id) DESC, u.id, c.id
            LIMIT $1
            """,
            ENTITY_LIMIT,
        )
    finally:
        await connection.close()

    if not rows:
        raise RuntimeError("No valid user/card pairs found in Postgres")
    return [(row["user_id"], row["card_id"]) for row in rows]


def next_entity() -> tuple[str, str]:
    global _entity_index

    with _entity_lock:
        if not _entities:
            raise RuntimeError("Locust entities have not been initialized")
        entity = _entities[_entity_index % len(_entities)]
        _entity_index += 1
        return entity


def build_payload(transaction_id: str, user_id: str, card_id: str) -> dict[str, Any]:
    return {
        "transaction_id": transaction_id,
        "event_timestamp": datetime.now(HO_CHI_MINH_TZ).replace(microsecond=0).isoformat(),
        "amount": 10.99,
        "channel": "C",
        "user_id": user_id,
        "card_id": card_id,
        "issuer_code": 404,
        "card_brand": "visa",
        "bin_code": 142,
        "card_type": "credit",
        "billing_zone": 1,
        "billing_country": 840,
        "email_purchaser": "yahoo.com",
        "email_recipient": "yahoo.com",
        "device_type": "mobile",
        "device_info": "Android 4.4",
        "os_raw": "Android 4.4",
        "browser_raw": "Chrome Mobile 30.0",
        "screen_resolution": "720x1280",
        "C1": 1,
        "C2": 1,
        "M1": "T",
        "M2": "T",
        "M6": "F",
    }


@events.test_start.add_listener
def load_entities(environment: Any, **_: Any) -> None:
    global _entities, _entity_index

    if isinstance(environment.runner, MasterRunner):
        return

    entities = asyncio.run(query_valid_entities())
    with _entity_lock:
        _entities = entities
        _entity_index = 0
    print(f"Loaded {len(_entities)} valid user/card pair(s) from Postgres")


class FraudDetectionApiUser(HttpUser):
    wait_time = constant_throughput(RPS_PER_USER)

    @task
    def score_transaction(self) -> None:
        user_id, card_id = next_entity()
        transaction_id = uuid4().hex
        payload = build_payload(transaction_id, user_id, card_id)

        with self.client.post("/score", json=payload, name="/score", catch_response=True) as response:
            if not response.ok:
                response.failure(f"HTTP {response.status_code}: {response.text[:240]}")
                return

            try:
                body = response.json()
            except ValueError as exc:
                response.failure(f"Invalid JSON response: {exc}")
                return

            if body.get("transaction_id") != transaction_id:
                response.failure("transaction_id mismatch")
