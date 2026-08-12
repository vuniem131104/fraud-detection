"""Đổ `warehouse.dwh.fact_transactions` (Postgres) vào Redis `vel:card:*`.

Ba feature `card_tx_count_5min` / `card_amount_sum_5min` / `card_amount_avg_5min`
KHÔNG được lưu thành giá trị trong Redis — chúng được TÍNH RA từ một sorted set
duy nhất mỗi thẻ, ngay trong đường score (xem
``src/fraud_detection/features/velocity.py``)::

    key    vel:card:{card_id}
    member "{transaction_id}|{amount_usd}"      <- amount nằm trong member
    score  epoch giây của created_at            <- event time, không phải now()

Script Lua ``LUA_CARD_VELOCITY`` dọn ngoài cửa sổ -> ZADD giao dịch hiện tại ->
ZRANGEBYSCORE ``[t-300, t]``, rồi đọc ra đúng ba feature trên (avg = sum/count,
count=0 -> avg=0). Nên "đổ ba feature vào Redis" = dựng lại đúng sorted set đó.

PHẠM VI MẶC ĐỊNH: 2 NGÀY
------------------------
Mặc định chỉ nạp từ **00:00 của ngày (hôm nay - 2)** — tức là chấp nhận giả định
"Redis chậm nhất là 2 ngày chưa cập nhật". Cửa sổ velocity chỉ có 300 giây, nên
nạp cả 90 ngày lịch sử không làm feature đúng hơn một chút nào: mọi member cũ hơn
300s so với giao dịch đang score đều bị ``ZREMRANGEBYSCORE`` xoá ở lần chạm đầu
tiên. Hai ngày là biên rộng rãi để bù độ trễ batch mà không phình key.

    --since-days N   đổi biên đó
    --since <ISO>    chốt cứng một mốc, thắng --since-days

Chạy::

    python scripts/backfill_vel_card.py                        # từ 00:00 (hôm nay-2)
    python scripts/backfill_vel_card.py --since 2026-08-10     # chốt cứng mốc
    python scripts/backfill_vel_card.py --since-days 7
    python scripts/backfill_vel_card.py --ttl 600              # TTL đúng KEY_TTL_S
    python scripts/backfill_vel_card.py --flush                # xoá vel:card:* trước
    python scripts/backfill_vel_card.py --verify 10            # in 3 feature 10 thẻ

Idempotent: ``ZADD`` cùng member chỉ cập nhật score, chạy lại bao nhiêu lần cũng
ra cùng một state.
"""

from __future__ import annotations

import argparse
import asyncio
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import asyncpg
from redis import asyncio as aioredis

REPO_ROOT = Path(__file__).resolve().parent.parent

# ===================================================================
# HỢP ĐỒNG VỚI ĐƯỜNG SERVING — khai lại tại chỗ, CỐ Ý không import
#
# Vì sao không `from fraud_detection.features import velocity`:
#
#   * Đây là script VẬN HÀNH, chạy bằng `python scripts/...` từ repo root, không
#     phải một phần của app. Import được velocity.py thì phải kèm
#     sys.path.insert("src") — và velocity.py lại tự đi tìm `feature_windows`
#     qua SHARED_DIR/fallback path. Một lần backfill không nên chết vì cái chuỗi
#     phụ thuộc đó gãy ở một mắt xích chẳng liên quan gì tới việc ghi Redis.
#   * Script phải copy đi chạy được ở nơi không có repo (VM, pod debug, psql box)
#     — bốn hằng số dưới đây là toàn bộ thứ nó cần biết.
#
# ĐÁNH ĐỔI, nói thẳng: giá trị bị nhân đôi. Đổi format member hay độ dài cửa sổ
# ở velocity.py mà quên file này thì KHÔNG có lỗi nào nổ ra — chỉ là state trong
# Redis sai âm thầm, và feature sai âm thầm là kiểu hỏng tệ nhất.
#
# NGUỒN CHUẨN vẫn là:
#   data_pipeline/shared/feature_windows.py  -> CARD_VELOCITY_WINDOW_S / _KEY_TTL_S
#   src/fraud_detection/features/velocity.py -> KEY_PREFIX, member(), key()
# Sửa bên đó thì phải sửa xuống đây.
# ===================================================================
KEY_PREFIX = "vel:card"      # = velocity.KEY_PREFIX (ngoài namespace của Feast)
WINDOW_S = 300               # = feature_windows.CARD_VELOCITY_WINDOW_S
KEY_TTL_S = 600              # = feature_windows.CARD_VELOCITY_KEY_TTL_S
MEMBER_SEP = "|"             # member = f"{txn_id}|{amount}", Lua parse phần sau '|'

PIPELINE_BATCH = 1000


def key(card_id: str) -> str:
    """Khoá Redis của một thẻ — phải khớp từng ký tự với ``velocity.key()``."""
    return f"{KEY_PREFIX}:{card_id}"


def member(txn_id: str, amount: float) -> str:
    """Member của sorted set — phải khớp từng ký tự với ``velocity.member()``.

    ``txn_id`` làm khoá dedup (ZADD cùng member = idempotent), ``amount`` nhét
    luôn vào member để script Lua tính tổng mà không cần cấu trúc thứ hai.
    """
    return f"{txn_id}{MEMBER_SEP}{amount}"


def velocity_of(members_in_window: list[str]) -> tuple[int, float, float]:
    """(count, sum, avg) từ danh sách member — cùng công thức với ``velocity.parse()``.

    ``count == 0 -> avg = 0.0`` chứ không phải chia cho 0: thẻ im lặng là trạng
    thái HỢP LỆ của feature này, không phải thiếu dữ liệu.
    """
    n = len(members_in_window)
    total = sum(float(m.rsplit(MEMBER_SEP, 1)[1]) for m in members_in_window)
    return n, total, (total / n if n else 0.0)


def _load_env() -> None:
    """Nạp .env vào os.environ (không ghi đè biến đã có sẵn ngoài shell)."""
    env_path = REPO_ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip())


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--since", default=None, metavar="ISO",
                   help="Mốc bắt đầu, ISO-8601 UTC (vd 2026-08-10 hoặc "
                        "2026-08-10T00:00:00). Thắng --since-days.")
    p.add_argument("--since-days", type=int, default=2, metavar="N",
                   help="Nạp từ 00:00 của ngày (hôm nay - N), UTC. Mặc định 2 = "
                        "chấp nhận Redis trễ tối đa 2 ngày.")
    p.add_argument("--ttl", type=int, default=0,
                   help=f"TTL của key, giây. 0 = không hết hạn (mặc định). "
                        f"Đường serving tự đặt lại {KEY_TTL_S}s khi chạm.")
    p.add_argument("--flush", action="store_true",
                   help=f"Xoá sạch {KEY_PREFIX}:* trước khi nạp (SCAN + UNLINK).")
    p.add_argument("--verify", type=int, default=5, metavar="N",
                   help="In 3 feature velocity của N thẻ nhiều giao dịch nhất "
                        "sau khi nạp. 0 = bỏ qua.")
    p.add_argument("--pg-db", default="warehouse", help="Database Postgres.")
    p.add_argument("--schema", default="dwh")
    p.add_argument("--table", default="fact_transactions")
    return p.parse_args()


def resolve_since(args: argparse.Namespace) -> datetime:
    """Mốc bắt đầu, trả về datetime NAIVE.

    Cột ``created_at`` là ``timestamp without time zone`` và TimeZone của DB là
    UTC, nên tham số so sánh cũng phải naive-UTC — truyền datetime có tzinfo vào
    asyncpg cho cột này là lỗi kiểu ngay tại driver.
    """
    if args.since:
        dt = datetime.fromisoformat(args.since)
        return dt.replace(tzinfo=None)
    midnight = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
    return midnight - timedelta(days=args.since_days)


async def fetch_rows(args: argparse.Namespace, since: datetime) -> list[asyncpg.Record]:
    """Đọc (id, card_id, amount_usd, epoch) từ bảng fact kể từ ``since``.

    ``extract(epoch FROM created_at)`` trên cột naive cho ra epoch UTC — cùng hệ
    quy chiếu với ``event_ts`` mà ``CardVelocity.compute`` nhận lúc serving.
    """
    conn = await asyncpg.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        database=args.pg_db,
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    try:
        return await conn.fetch(
            f"""
            SELECT id, card_id, amount_usd,
                   extract(epoch FROM created_at) AS epoch
            FROM "{args.schema}"."{args.table}"
            WHERE created_at >= $1
            """,
            since,
        )
    finally:
        await conn.close()


async def flush_prefix(redis_client) -> int:
    """UNLINK mọi key vel:card:* — SCAN chứ không KEYS (không chặn Redis)."""
    removed = 0
    batch: list[str] = []
    async for k in redis_client.scan_iter(match=f"{KEY_PREFIX}:*", count=1000):
        batch.append(k)
        if len(batch) >= PIPELINE_BATCH:
            removed += await redis_client.unlink(*batch)
            batch.clear()
    if batch:
        removed += await redis_client.unlink(*batch)
    return removed


async def load(redis_client, per_card: dict[str, dict[str, float]], ttl: int) -> None:
    """ZADD từng thẻ theo pipeline. ZADD idempotent theo member -> chạy lại vô hại."""
    pipe = redis_client.pipeline(transaction=False)
    pending = 0
    for card_id, members in per_card.items():
        k = key(card_id)
        pipe.zadd(k, members)
        pending += 1
        if ttl > 0:
            pipe.expire(k, ttl)
            pending += 1
        if pending >= PIPELINE_BATCH:
            await pipe.execute()
            pending = 0
    if pending:
        await pipe.execute()


async def verify(redis_client, per_card: dict[str, dict[str, float]], top_n: int) -> None:
    """Đọc lại từ Redis và tính 3 feature.

    Mốc thời gian là giao dịch MỚI NHẤT của chính thẻ đó (không phải now()): đây
    là câu hỏi "nếu giao dịch cuối cùng vừa xảy ra thì model nhìn thấy gì".
    """
    ranked = sorted(per_card.items(), key=lambda kv: len(kv[1]), reverse=True)[:top_n]
    if not ranked:
        return
    print(f"\nverify — {WINDOW_S}s tính từ giao dịch mới nhất của mỗi thẻ:")
    print(f"  {'card_id':<34} {'count':>6} {'sum':>12} {'avg':>10}")
    for card_id, members in ranked:
        t = max(members.values())
        raw = await redis_client.zrangebyscore(key(card_id), t - WINDOW_S, t)
        n, total, avg = velocity_of(raw)
        print(f"  {card_id:<34} {n:>6} {total:>12.2f} {avg:>10.2f}")


async def main() -> None:
    args = parse_args()
    _load_env()

    since = resolve_since(args)
    rows = await fetch_rows(args, since)

    per_card: dict[str, dict[str, float]] = {}
    for row in rows:
        per_card.setdefault(row["card_id"], {})[
            member(row["id"], row["amount_usd"])] = float(row["epoch"])

    print(f"postgres: {len(rows)} giao dịch / {len(per_card)} thẻ "
          f"— từ {since.isoformat(sep=' ')} UTC")

    redis_client = aioredis.Redis(
        host=os.environ["REDIS_HOST"],
        port=int(os.environ["REDIS_PORT"]),
        db=int(os.getenv("REDIS_DB", "0")),
        decode_responses=True,
    )
    try:
        if args.flush:
            print(f"redis: UNLINK {await flush_prefix(redis_client)} key {KEY_PREFIX}:*")
        await load(redis_client, per_card, args.ttl)
        ttl_note = "không TTL" if args.ttl <= 0 else f"TTL {args.ttl}s"
        print(f"redis: {len(per_card)} key {KEY_PREFIX}:* "
              f"({len(rows)} member, {ttl_note})")
        if args.verify:
            await verify(redis_client, per_card, args.verify)
    finally:
        await redis_client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
