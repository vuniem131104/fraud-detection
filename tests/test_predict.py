import os
import sys
from datetime import UTC, datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from redis import asyncio as aioredis
import asyncio
import json
from uuid import uuid4

from database import PostgresDatabase
from fraud_detection.core.predict import FraudDetectionService
from fraud_detection.core.models import FraudDetectionInputs
from fraud_detection.features.feature_store import RedisFeatureStore
from structlog import get_logger
from typing import Any

logger = get_logger(__name__)

def load_schema(model_dir: str) -> dict[str, Any]:
    schema_path = Path(model_dir)  / "feature_schema.json"
    if schema_path.exists():
        return json.loads(schema_path.read_text())

    raise FileNotFoundError(f"Feature schema not found")

def issuer_numeric(issuer_code: str) -> int:
    digits = "".join(character for character in issuer_code if character.isdigit())
    return int(digits or 0)

async def load_card_profile(
    database: PostgresDatabase,
    user_id: str,
    card_id: str,
) -> dict[str, Any]:
    async with database.connection() as conn:
        row = await conn.fetchrow(
            """
            SELECT
                c.issuer_code,
                c.country AS card_country,
                c.brand AS card_brand,
                c.bin_code,
                c.type AS card_type,
                t.email_purchaser
            FROM application.cards AS c
            JOIN LATERAL (
                SELECT email_purchaser
                FROM application.transactions
                WHERE user_id = c.user_id
                  AND card_id = c.id
                ORDER BY created_at DESC
                LIMIT 1
            ) AS t ON TRUE
            WHERE c.user_id = $1
              AND c.id = $2
            """,
            user_id,
            card_id,
        )

    if row is None:
        raise ValueError(f"No transaction history found for user_id={user_id}, card_id={card_id}")
    return dict(row)

def build_anomalous_current_transaction(
    user_id: str,
    card_id: str,
    card_profile: dict[str, Any],
    redis_state: dict[str, Any],
    event_timestamp: str | None = None,
) -> dict[str, Any]:
    transactions = redis_state.get("transactions", [])
    if not transactions:
        raise ValueError(f"No Redis transaction history found for user_id={user_id}, card_id={card_id}")

    timestamp = (
        event_timestamp
        or datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    )
    tx_id = uuid4().hex

    return {
        "tx_id": tx_id,
        "user_id": user_id,
        "card_id": card_id,
        "amount_usd": 1_000_000_000.0,
        "channel": "C",
        "issuer_code": 99999 if issuer_numeric(card_profile["issuer_code"]) != 99999 else 404,
        "card_brand": "discover" if card_profile["card_brand"] != "discover" else "visa",
        "card_country": 840,
        "bin_code": "468878",
        "card_type": "credit" if card_profile["card_type"] != "credit" else "debit",
        "billing_zone": 999,
        "billing_country": card_profile["card_country"],
        "email_purchaser": "buyer@gmail.com",
        "email_recipient": f"cashout.{card_id}@protonmail.com",
        "device_type": "missing",
        "device_info": "UnknownRootedAndroid X999",
        "os_raw": "Windows 11",
        "browser_raw": "chrome 80.0",
        "screen_resolution": "1366x768",
        "event_timestamp": timestamp,
        "C1": 45,
        "C2": 39,
        "C13": 1,
        # "D4": 0.00001,
        # "D15": 7.0,
        "M1": "F",
        "M2": "T",
        "M6": "F",
    }

async def main_async() -> int:
    user_id = "6cf25546617f4aa3b388f8cabb224ca7"
    card_id = "9a2b20f41614408c97ac7ddea07811e7"
    redis_client = aioredis.Redis(
        host=os.getenv("REDIS_HOST"),
        port=int(os.getenv("REDIS_PORT")),
        db=int(os.getenv("REDIS_DB" )),
        decode_responses=True,
    )
    postgres_client = PostgresDatabase.from_env()
    schema = load_schema(model_dir="/home/lehoangvu/fraud-detection/models")
    service: FraudDetectionService | None = None
    try:
        await postgres_client.open()
        await redis_client.ping()
        feature_store = RedisFeatureStore(redis_client)
        card_profile = await load_card_profile(postgres_client, user_id, card_id)
        redis_state = await feature_store.get_txs(user_id, card_id)
        service = FraudDetectionService(
            schema=schema,
            feature_store=feature_store,
            database=postgres_client,
            threshold=0.5,
        )
        current_transaction = build_anomalous_current_transaction(
            user_id=user_id,
            card_id=card_id,
            card_profile=card_profile,
            redis_state=redis_state,
        )
        details = await service.predict(
            FraudDetectionInputs(**current_transaction)
        )
        if details.status != "review":
            raise AssertionError(
                f"Expected anomalous transaction to be reviewed, got status={details.status}, "
                f"probability={details.probability:.6f}"
            )
        if details.probability < 0.85:
            raise AssertionError(
                f"Expected anomalous transaction probability >= 0.85, "
                f"got {details.probability:.6f}"
            )
    except Exception:
        logger.exception(
            "Fraud prediction failed",
            extra={
                "user_id": user_id,
                "card_id": card_id,
            },
        )
        raise
    finally:
        if service is not None:
            await service.close()
        await postgres_client.close()
        await redis_client.aclose()

    result = {
        "tx_id": details.tx_id,
        "probability": round(details.probability, 6),
        "status": details.status,
    }
    print(json.dumps(result, indent=2))
    return 0


def main() -> int:
    return asyncio.run(main_async())


if __name__ == "__main__":
    raise SystemExit(main())
