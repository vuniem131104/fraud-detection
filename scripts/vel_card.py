"""In sorted set velocity của MỘT thẻ.

    python scripts/vel_card.py <card_id>

CHỈ ĐỌC — không ``ZADD``, không ``EXPIRE``, không dọn key. Đường serving thật
(``CardVelocity.compute``) ghi giao dịch hiện tại TRƯỚC rồi mới đọc; một lệnh tra
cứu mà cũng ghi thì lần score kế tiếp đã lệch sẵn.

In TOÀN BỘ set, không lọc theo cửa sổ 300s: đây là công cụ xem state thô. Muốn
biết model nhìn thấy gì thì lấy các dòng trong 300s cuối tính từ giao dịch đang
score.
"""

from __future__ import annotations

import asyncio
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from redis import asyncio as aioredis

REPO_ROOT = Path(__file__).resolve().parent.parent

# ===================================================================
# HỢP ĐỒNG VỚI ĐƯỜNG SERVING — khai lại tại chỗ, CỐ Ý không import
#
# Giống lý do ở scripts/backfill_vel_card.py: đây là công cụ vận hành, chạy bằng
# `python scripts/...`, không phải một phần của app. Import velocity.py thì kéo
# theo sys.path.insert("src") + chuỗi đi tìm `feature_windows` — một lệnh tra cứu
# không nên chết vì mắt xích chẳng liên quan gì tới việc đọc Redis, và file này
# phải copy đi chạy được ở pod/VM không có repo.
#
# ĐÁNH ĐỔI: giá trị bị nhân đôi, đổi bên kia mà quên bên này thì không lỗi nào nổ
# ra — chỉ là số in ra sai âm thầm. Nguồn chuẩn:
#   src/fraud_detection/features/velocity.py -> KEY_PREFIX, member()
# ===================================================================
KEY_PREFIX = "vel:card"      # = velocity.KEY_PREFIX
MEMBER_SEP = "|"             # member = f"{txn_id}|{amount}"


def _load_env() -> None:
    env_path = REPO_ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip())


async def main() -> None:
    if len(sys.argv) != 2:
        sys.exit("usage: python scripts/vel_card.py <card_id>")
    card_id = sys.argv[1]
    _load_env()

    key = f"{KEY_PREFIX}:{card_id}"
    redis_client = aioredis.Redis(
        host=os.environ["REDIS_HOST"],
        port=int(os.environ["REDIS_PORT"]),
        db=int(os.getenv("REDIS_DB", "0")),
        decode_responses=True,
    )
    try:
        rows = await redis_client.zrange(key, 0, -1, withscores=True)
    finally:
        await redis_client.aclose()

    print(key)
    if not rows:
        # Không phải lỗi: thẻ chưa từng quẹt, hoặc key đã hết TTL. Với velocity
        # thì trạng thái đó CÓ NGHĨA — cả ba feature là 0.
        print("  (rỗng)")
        return

    print(f"  {'score':>17}  {'created_at (UTC)':<19}  {'txn_id':<32} {'amount_usd':>10}")
    for member, score in rows:
        txn_id, amount = member.rsplit(MEMBER_SEP, 1)
        ts = datetime.fromtimestamp(score, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        print(f"  {score:>17.3f}  {ts:<19}  {txn_id:<32} {float(amount):>10.2f}")
    print(f"  -> {len(rows)} giao dịch")


if __name__ == "__main__":
    asyncio.run(main())
