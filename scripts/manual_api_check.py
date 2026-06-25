"""
test_api.py – call the fraud-detection API with a normal or anomalous payload.

Usage:
    # Giao dịch bình thường (1 lần):
    python3 tests/test_api.py <user_id> <card_id>

    # Giao dịch bất thường – gửi liên tục đến khi model đoán là fraud (prediction=1):
    python3 tests/test_api.py <user_id> <card_id> --anomalous
    python3 tests/test_api.py <user_id> <card_id> --anomalous --max-attempts 200
"""

import argparse
import json
import os
import random
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import httpx

# ── DB (đọc thông tin card từ Postgres) ──────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

API_URL = "http://localhost:1311/score"
HO_CHI_MINH_TZ = timezone(timedelta(hours=7), "Asia/Ho_Chi_Minh")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_dotenv() -> None:
    """Load key/value pairs from the project ``.env`` file into the environment if present."""
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
    """Load the .env file and parse CLI arguments (user/card, --anomalous, --max-attempts)."""
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Test the fraud-detection API.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("user_id")
    parser.add_argument("card_id")
    parser.add_argument(
        "--anomalous",
        action="store_true",
        help=(
            "Gửi payload bất thường liên tục đến khi model đoán là fraud (prediction=1)."
        ),
    )
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=500,
        metavar="N",
        help="Số lần thử tối đa khi dùng --anomalous (mặc định: 500).",
    )
    return parser.parse_args()


def fetch_card_profile(user_id: str, card_id: str) -> dict:
    """Lấy thông tin card từ Postgres để build payload chính xác."""
    import asyncio
    import asyncpg

    async def _query():
        """Connect to Postgres and fetch the card profile row for the user/card pair."""
        conn = await asyncpg.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD", ""),
            database=os.getenv("POSTGRES_DB", "fraud-detection"),
        )
        try:
            row = await conn.fetchrow(
                """
                SELECT
                    c.issuer_code,
                    c.country  AS card_country,
                    c.brand    AS card_brand,
                    c.bin_code,
                    c.type     AS card_type,
                    COALESCE(t.email_purchaser, u.email) AS email_purchaser
                FROM application.cards AS c
                JOIN application.users AS u ON u.id = c.user_id
                LEFT JOIN LATERAL (
                    SELECT email_purchaser
                    FROM application.transactions
                    WHERE user_id = c.user_id AND card_id = c.id
                    ORDER BY created_at DESC
                    LIMIT 1
                ) AS t ON TRUE
                WHERE c.user_id = $1 AND c.id = $2
                """,
                user_id,
                card_id,
            )
        finally:
            await conn.close()
        return dict(row) if row else {}

    return asyncio.run(_query())


def build_normal_payload(user_id: str, card_id: str) -> dict:
    """Build a typical, low-risk transaction payload for the given user/card pair."""
    return {
        "tx_id":             uuid4().hex,
        "user_id":           user_id,
        "card_id":           card_id,
        "event_timestamp":   datetime.now(HO_CHI_MINH_TZ).replace(microsecond=0).isoformat(),
        "amount_usd":        50.0,
        "channel":           "W",
        "card_country":      840,
        "issuer_code":       84001,
        "card_brand":        "visa",
        "bin_code":          "411111",
        "card_type":         "credit",
        "billing_zone":      1,
        "billing_country":   840,
        "email_purchaser":   "buyer@gmail.com",
        "email_recipient":   "seller@gmail.com",
        "device_type":       "desktop",
        "device_info":       "desktop:Windows 11:Chrome",
        "os_raw":            "Windows 11",
        "browser_raw":       "Chrome",
        "screen_resolution": "1920x1080",
        "C1":  5,
        "C2":  2,
        "C13": 10,
        "M1":  "T",
        "M2":  "T",
        "M6":  "F",
        "D4":  3.0,
        "D15": 7.0,
    }


def build_anomalous_payload(user_id: str, card_id: str, card_profile: dict) -> dict:
    """
    Payload bất thường dựa trên pattern đã biết cho score cao.
    Mỗi lần gọi sẽ sinh ra một billing_zone và email ngẫu nhiên để
    uid luôn là mới (chưa có trong Redis) → uid aggregation = null,
    C13=0 thực sự có tác dụng mỗi lần.
    """
    card_brand   = card_profile.get("card_brand", "visa")
    card_type    = card_profile.get("card_type",  "credit")
    card_country = card_profile.get("card_country", 840)
    raw_issuer   = str(card_profile.get("issuer_code", "0"))
    digits       = "".join(c for c in raw_issuer if c.isdigit())
    issuer_num   = int(digits or 0)

    # billing_zone ngẫu nhiên trong dải lạ (10000-99999) → uid1 luôn mới
    fresh_zone = random.randint(10_000, 99_999)
    # subdomain khác mỗi lần để uid3 cũng mới
    rand_tag   = uuid4().hex[:8]

    return {
        "tx_id":             uuid4().hex,
        "user_id":           user_id,
        "card_id":           card_id,
        "event_timestamp":   datetime.now(HO_CHI_MINH_TZ).replace(microsecond=0).isoformat(),
        # amount cực lớn → amount_zscore_card cao
        "amount_usd":        1_000_000_000.0,
        # mobile app channel (enc=2)
        "channel":           "C",
        # issuer hoàn toàn khác thẻ thực
        "issuer_code":       99999 if issuer_num != 99999 else 404,
        # brand mismatch
        "card_brand":        "discover" if card_brand != "discover" else "visa",
        "card_country":      840,
        "bin_code":          "468878",
        # type mismatch
        "card_type":         "credit" if card_type != "credit" else "debit",
        # billing_zone ngẫu nhiên → uid1 chưa từng thấy → uid aggregation null
        "billing_zone":      fresh_zone,
        "billing_country":   card_country,
        "email_purchaser":   "buyer@gmail.com",
        # protonmail (enc=56, rare) – subdomain khác mỗi lần → uid3 luôn mới
        "email_recipient":   f"cashout.{rand_tag}@protonmail.com",
        # device unknown/rooted → device_info enc cao, freq thấp
        "device_type":       "missing",
        "device_info":       "UnknownRootedAndroid X999",
        "os_raw":            "Windows 11",
        "browser_raw":       "chrome 80.0",
        "screen_resolution": "1366x768",
        # C1/C2 cao
        "C1":  45,
        "C2":  39,
        # C13=0 → uid mới, không có lịch sử
        "C13": 0,
        "M1":  "F",
        "M2":  "T",
        "M6":  "F",
        "D4":  0.0,
        "D15": 0.0,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    """Send a single normal payload, or repeatedly send anomalous payloads until fraud is predicted."""
    args = parse_args()

    if not args.anomalous:
        # ── Chế độ thường: gửi 1 lần ──────────────────────────────────────
        payload = build_normal_payload(args.user_id, args.card_id)
        print(f"POST {API_URL}")
        print(json.dumps(payload, indent=2))

        response = httpx.post(API_URL, json=payload, timeout=30.0)
        response.raise_for_status()

        print("\nResponse:")
        print(json.dumps(response.json(), indent=2))
        return 0

    # ── Chế độ --anomalous ────────────────────────────────────────────────
    print(f"[anomalous] Đang lấy thông tin card từ DB...")
    try:
        card_profile = fetch_card_profile(args.user_id, args.card_id)
        if not card_profile:
            print("  ⚠  Không tìm thấy card trong DB – dùng giá trị mặc định.")
            card_profile = {}
        else:
            print(f"  Card profile: {card_profile}")
    except Exception as exc:
        print(f"  ⚠  Lỗi khi lấy card profile ({exc}) – dùng giá trị mặc định.")
        card_profile = {}

    print(f"\n[anomalous] Bắt đầu gửi (tối đa {args.max_attempts} lần)…\n")

    for attempt in range(1, args.max_attempts + 1):
        payload = build_anomalous_payload(args.user_id, args.card_id, card_profile)

        try:
            response = httpx.post(API_URL, json=payload, timeout=30.0)
            response.raise_for_status()
        except httpx.HTTPError as exc:
            print(f"  [attempt {attempt:>4}] HTTP error: {exc}")
            time.sleep(0.5)
            continue

        data        = response.json()
        probability = data.get("probability", 0.0)
        prediction  = data.get("prediction",  0)

        print(
            f"  [attempt {attempt:>4}] "
            f"probability={probability:.4f} | prediction={prediction}"
        )

        if prediction == 1:
            print("\n✅ Model đã đoán là FRAUD (prediction=1)!")
            print("\nPayload:")
            print(json.dumps(payload, indent=2))
            print("\nResponse:")
            print(json.dumps(data, indent=2))
            return 0

        time.sleep(0.1)

    print(f"\n❌ Đã thử {args.max_attempts} lần nhưng model vẫn chưa đoán là fraud.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
