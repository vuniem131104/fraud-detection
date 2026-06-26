"""
test_api.py – gửi giao dịch bình thường tới fraud-detection API.

Usage:
    # 1 giao dịch (1 user + card):
    python3 test.py <user_id>:<card_id>

    # Bắn N request CÙNG LÚC, chia đều cho các cặp user_id:card_id (test autoscaling):
    python3 test.py u1:c1 u2:c2 u3:c3 u4:c4 u5:c5 -n 50
"""

import argparse
import asyncio
import json
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import httpx

API_URL = "http://localhost:1311/score"
HO_CHI_MINH_TZ = timezone(timedelta(hours=7), "Asia/Ho_Chi_Minh")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Test the fraud-detection API.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "pairs",
        nargs="+",
        metavar="user_id:card_id",
        help="Một hoặc nhiều cặp user_id:card_id.",
    )
    parser.add_argument(
        "-n",
        "--num",
        type=int,
        default=1,
        metavar="N",
        help="Số request bắn cùng lúc, chia đều cho các cặp (mặc định 1).",
    )
    return parser.parse_args()


def parse_pairs(raw_pairs: list[str]) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    for raw in raw_pairs:
        user_id, _, card_id = raw.partition(":")
        if not user_id or not card_id:
            raise SystemExit(f"Cặp không hợp lệ: {raw!r} (định dạng đúng: user_id:card_id)")
        pairs.append((user_id, card_id))
    return pairs


def build_normal_payload(user_id: str, card_id: str) -> dict:
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


async def send_one(client: httpx.AsyncClient, user_id: str, card_id: str) -> dict:
    response = await client.post(API_URL, json=build_normal_payload(user_id, card_id))
    response.raise_for_status()
    return response.json()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def run() -> int:
    args = parse_args()
    pairs = parse_pairs(args.pairs)

    # ── 1 request: 1 user + card, in đầy đủ payload + response ────────────
    if args.num <= 1 and len(pairs) == 1:
        user_id, card_id = pairs[0]
        payload = build_normal_payload(user_id, card_id)
        print(f"POST {API_URL}")
        print(json.dumps(payload, indent=2))

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(API_URL, json=payload)
            response.raise_for_status()
            print("\nResponse:")
            print(json.dumps(response.json(), indent=2))
        return 0

    # ── Bắn N request cùng lúc, round-robin các cặp ───────────────────────
    print(f"Bắn {args.num} request cùng lúc, chia đều cho {len(pairs)} cặp → {API_URL}")
    limits = httpx.Limits(max_connections=min(args.num, 200), max_keepalive_connections=50)
    async with httpx.AsyncClient(timeout=30.0, limits=limits) as client:
        tasks = [
            send_one(client, *pairs[i % len(pairs)])
            for i in range(args.num)
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    n_ok = sum(1 for r in results if not isinstance(r, Exception))
    n_err = args.num - n_ok
    print(f"\nKết quả: ok={n_ok} err={n_err}")
    first_err = next((r for r in results if isinstance(r, Exception)), None)
    if first_err is not None:
        print(f"  ví dụ lỗi: {first_err!r}")
    return 1 if n_ok == 0 else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run()))
