import argparse
import asyncio
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

from redis import asyncio as aioredis
from structlog import get_logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from database import PostgresDatabase
from fraud_detection.core.models import FraudDetectionInputs
from fraud_detection.core.predict import FraudDetectionService
from fraud_detection.features.feature_store import RedisFeatureStore

logger = get_logger(__name__)
HO_CHI_MINH_TZ = timezone(timedelta(hours=7), "Asia/Ho_Chi_Minh")
Mode = Literal["with-transactions", "without-transactions"]


@dataclass(frozen=True)
class PredictionCase:
    mode: Mode
    user_id: str
    card_id: str
    redis_state: dict[str, Any]

    @property
    def previous_transaction_count(self) -> int:
        transactions = self.redis_state.get("transactions", [])
        return len(transactions) if isinstance(transactions, list) else 0


def load_dotenv() -> None:
    env_path = PROJECT_ROOT / ".env"
    if not env_path.exists():
        return

    for raw_line in env_path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, value = line.split("=", 1)
        os.environ.setdefault(name.strip(), value.strip().strip("\"'"))


def parse_args() -> argparse.Namespace:
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Run fraud prediction smoke checks with or without prior transactions."
    )
    parser.add_argument("user_id", help="User id to score.")
    parser.add_argument("card_id", help="Card id to score.")
    transaction_group = parser.add_mutually_exclusive_group()
    transaction_group.add_argument(
        "--with-transactions",
        dest="with_transactions",
        action="store_true",
        default=True,
        help="Require previous transactions for this user/card pair. This is the default.",
    )
    transaction_group.add_argument(
        "--without-transactions",
        dest="with_transactions",
        action="store_false",
        help="Require no previous transactions for this user/card pair.",
    )
    parser.add_argument(
        "--model-dir",
        default=os.getenv("MODEL_DIR", str(PROJECT_ROOT / "models")),
        help="Directory containing feature_schema.json.",
    )
    parser.add_argument(
        "--min-history-review-probability",
        type=float,
        default=float(os.getenv("PREDICT_MIN_HISTORY_REVIEW_PROBABILITY", "0")),
        help="Optional minimum review probability for the with-transactions anomalous case.",
    )
    return parser.parse_args()


def load_schema(model_dir: str) -> dict[str, Any]:
    schema_path = Path(model_dir) / "feature_schema.json"
    if schema_path.exists():
        return json.loads(schema_path.read_text())

    raise FileNotFoundError("Feature schema not found")


def issuer_numeric(issuer_code: str) -> int:
    digits = "".join(character for character in issuer_code if character.isdigit())
    return int(digits or 0)


async def transaction_count(
    database: PostgresDatabase,
    user_id: str,
    card_id: str,
) -> int | None:
    async with database.connection() as conn:
        row = await conn.fetchrow(
            """
            SELECT
                COUNT(c.id) AS card_count,
                COUNT(t.id) AS transaction_count
            FROM application.cards AS c
            LEFT JOIN application.transactions AS t
                ON t.user_id = c.user_id
               AND t.card_id = c.id
            WHERE c.user_id = $1
              AND c.id = $2
            """,
            user_id,
            card_id,
        )

    if row is None or int(row["card_count"]) == 0:
        return None
    return int(row["transaction_count"])


def redis_state_matches_mode(redis_state: dict[str, Any], mode: Mode) -> bool:
    transactions = redis_state.get("transactions", [])
    transaction_count = len(transactions) if isinstance(transactions, list) else 0
    if mode == "with-transactions":
        return transaction_count > 0
    return transaction_count == 0


async def resolve_prediction_case(
    database: PostgresDatabase,
    feature_store: RedisFeatureStore,
    user_id: str,
    card_id: str,
    mode: Mode,
) -> PredictionCase:
    count = await transaction_count(database, user_id, card_id)
    if count is None:
        raise ValueError(f"No card found for user_id={user_id}, card_id={card_id}")
    if mode == "with-transactions" and count == 0:
        raise ValueError(f"Expected Postgres history for user_id={user_id}, card_id={card_id}")
    if mode == "without-transactions" and count > 0:
        raise ValueError(f"Expected no Postgres history for user_id={user_id}, card_id={card_id}")

    redis_state = await feature_store.get_txs(user_id, card_id)
    if not redis_state_matches_mode(redis_state, mode):
        raise ValueError(f"Redis history does not match mode={mode} for user_id={user_id}, card_id={card_id}")
    return PredictionCase(mode, user_id, card_id, redis_state)


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
                c.created_at AS card_created_at,
                COALESCE(t.email_purchaser, u.email) AS email_purchaser
            FROM application.cards AS c
            JOIN application.users AS u
                ON u.id = c.user_id
            LEFT JOIN LATERAL (
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
        raise ValueError(f"No card found for user_id={user_id}, card_id={card_id}")
    return dict(row)


def build_anomalous_current_transaction(
    prediction_case: PredictionCase,
    card_profile: dict[str, Any],
    event_timestamp: str | None = None,
) -> dict[str, Any]:
    if prediction_case.mode == "with-transactions" and prediction_case.previous_transaction_count == 0:
        raise ValueError(
            f"No Redis transaction history found for user_id={prediction_case.user_id}, "
            f"card_id={prediction_case.card_id}"
        )
    if prediction_case.mode == "without-transactions" and prediction_case.previous_transaction_count > 0:
        raise ValueError(
            f"Unexpected Redis transaction history found for user_id={prediction_case.user_id}, "
            f"card_id={prediction_case.card_id}"
        )

    timestamp = event_timestamp or datetime.now(HO_CHI_MINH_TZ).replace(microsecond=0).isoformat()
    tx_id = uuid4().hex

    return {
        "tx_id": tx_id,
        "user_id": prediction_case.user_id,
        "card_id": prediction_case.card_id,
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
        "email_recipient": f"cashout.{prediction_case.card_id}@protonmail.com",
        "device_type": "missing",
        "device_info": "UnknownRootedAndroid X999",
        "os_raw": "Windows 11",
        "browser_raw": "chrome 80.0",
        "screen_resolution": "1366x768",
        "event_timestamp": timestamp,
        "C1": 45,
        "C2": 39,
        "C13": 0,
        "M1": "F",
        "M2": "T",
        "M6": "F",
    }


def validate_prediction(
    mode: Mode,
    probability: float,
    status: str,
    min_history_review_probability: float,
) -> None:
    if not 0 <= probability <= 1:
        raise AssertionError(f"Expected probability in [0, 1], got {probability:.6f}")
    if mode == "with-transactions":
        if status != "review":
            raise AssertionError(
                f"Expected anomalous transaction with previous transactions to be reviewed, got "
                f"status={status}, probability={probability:.6f}"
            )
        if min_history_review_probability > 0 and probability < min_history_review_probability:
            raise AssertionError(
                f"Expected anomalous transaction with previous transactions probability >= "
                f"{min_history_review_probability:.2f}, got {probability:.6f}"
            )


async def run_prediction_case(
    mode: Mode,
    user_id: str,
    card_id: str,
    service: FraudDetectionService,
    database: PostgresDatabase,
    feature_store: RedisFeatureStore,
    min_history_review_probability: float,
) -> dict[str, Any]:
    prediction_case = await resolve_prediction_case(
        database,
        feature_store,
        user_id,
        card_id,
        mode,
    )
    card_profile = await load_card_profile(
        database,
        prediction_case.user_id,
        prediction_case.card_id,
    )
    current_transaction = build_anomalous_current_transaction(
        prediction_case=prediction_case,
        card_profile=card_profile,
    )
    details = await service.predict(FraudDetectionInputs(**current_transaction))
    validate_prediction(
        mode=mode,
        probability=details.probability,
        status=details.status,
        min_history_review_probability=min_history_review_probability,
    )
    return {
        "mode": mode,
        "user_id": prediction_case.user_id,
        "card_id": prediction_case.card_id,
        "previous_transactions": prediction_case.previous_transaction_count,
        "tx_id": details.tx_id,
        "probability": round(details.probability, 6),
        "status": details.status,
    }


async def main_async(args: argparse.Namespace) -> int:
    mode: Mode = "with-transactions" if args.with_transactions else "without-transactions"
    redis_client = aioredis.Redis(
        host=os.getenv("REDIS_HOST"),
        port=int(os.getenv("REDIS_PORT")),
        db=int(os.getenv("REDIS_DB")),
        decode_responses=True,
    )
    postgres_client = PostgresDatabase.from_env()
    schema = load_schema(model_dir=args.model_dir)
    service: FraudDetectionService | None = None
    try:
        feature_store = RedisFeatureStore(redis_client)
        service = FraudDetectionService(
            schema=schema,
            feature_store=feature_store,
            database=postgres_client,
            threshold=0.5,
        )
        await service.open()
        result = await run_prediction_case(
            mode=mode,
            user_id=args.user_id,
            card_id=args.card_id,
            service=service,
            database=postgres_client,
            feature_store=feature_store,
            min_history_review_probability=args.min_history_review_probability,
        )
    except Exception:
        logger.exception(
            "Fraud prediction failed",
            extra={
                "mode": mode,
                "user_id": args.user_id,
                "card_id": args.card_id,
            },
        )
        raise
    finally:
        if service is not None:
            await service.close()
        else:
            await postgres_client.close()
            await redis_client.aclose()

    print(json.dumps(result, indent=2))
    return 0


def main() -> int:
    return asyncio.run(main_async(parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
