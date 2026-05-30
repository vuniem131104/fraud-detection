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
from fraud_detection.core.models import FraudDetectionInputs, FraudDetectionOutputs
from fraud_detection.features.feature_store import RedisFeatureStore
from structlog import get_logger
from typing import Any

logger = get_logger(__name__)

def load_schema(model_dir: str) -> dict[str, Any]:
    schema_path = Path(model_dir)  / "feature_schema.json"
    if schema_path.exists():
        return json.loads(schema_path.read_text())

    raise FileNotFoundError(f"Feature schema not found")

def build_fake_current_transaction(
    user_id: str,
    card_id: str,
    amount: float = 10.99,
    channel: str = "C",
    event_timestamp: str | None = None,
) -> dict[str, Any]:
    timestamp = (
        event_timestamp
        or datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    )
    transaction_id = uuid4().hex

    return {
        "transaction_id": transaction_id,
        "user_id": user_id,
        "card_id": card_id,
        "amount": amount,
        "channel": channel,
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
        "event_timestamp": timestamp,
        "C1": 1,
        "C2": 1,
        "M1": "T",
        "M2": "T",
        "M6": "F",
    }

async def main_async() -> int:
    user_id = "dc7e1d88bf2945ee9b09a9fc43f94fa3"
    card_id = "645d259d535c4340b8d94d13828bba7b"
    redis_client = aioredis.Redis(
        host=os.getenv("REDIS_HOST"),
        port=int(os.getenv("REDIS_PORT")),
        db=int(os.getenv("REDIS_DB" )),
        decode_responses=True,
    )
    postgres_client = PostgresDatabase.from_env()
    schema = load_schema(model_dir="/home/lehoangvu/fraud-detection/models")
    try:
        await postgres_client.open()
        service = FraudDetectionService(
            schema=schema,
            feature_store=RedisFeatureStore(redis_client),
            database=postgres_client,
            threshold=0.5,
        )
        current_transaction = build_fake_current_transaction(user_id=user_id, card_id=card_id)
        details = await service.predict(
            FraudDetectionInputs(**current_transaction)
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
        await postgres_client.close()
        await redis_client.aclose()

    result = {
        "transaction_id": details.transaction_id,
        "probability": round(details.probability, 6),
        "status": details.status,
    }
    print(json.dumps(result, indent=2))
    return 0


def main() -> int:
    return asyncio.run(main_async())


if __name__ == "__main__":
    raise SystemExit(main())
