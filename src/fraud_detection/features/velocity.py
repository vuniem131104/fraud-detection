"""Velocity 5 phút của thẻ — tính ĐỒNG BỘ trong đường score bằng Redis sorted set.

Vì sao không dùng Flink cho nhóm feature này
--------------------------------------------
Flink có độ trễ cố hữu ``slide + watermark`` ≈ 2,5 phút (window chỉ phát ra sau khi
watermark vượt ``window_end``). Card testing là 12-45 giao dịch cách nhau 4-90
GIÂY: trễ 2,5 phút làm mất đúng cái burst.

Đo được trên hệ này: giao dịch lúc 10:06:30, giá trị đúng là 8, Flink trả 4.

Vì sao sorted set giải quyết trọn vẹn
-------------------------------------
====================  ==========================================================
Không có độ trễ       Counter chứa giao dịch hiện tại NGAY tại thời điểm score.
Đúng định nghĩa       ``[t-300, t]`` giống hệt ``rangeBetween(-300, 0)`` của Spark
                      -> train và serve dùng một công thức.
Tự về 0               ``ZREMRANGEBYSCORE`` xoá phần ngoài cửa sổ; thẻ im lặng ->
                      set rỗng -> 0 tự nhiên. Không cần kiểm tra độ tươi như với
                      giá trị Flink (thứ không bao giờ emit row cho window rỗng).
Đúng thứ tự           Read-modify-write nằm TRONG request path nên giao dịch N+1
                      luôn thấy giao dịch N. Card testing cách nhau vài giây vẫn bắt.
====================  ==========================================================

Điểm chết người: GHI TRƯỚC, ĐỌC SAU
-----------------------------------
``rangeBetween(-300, 0)`` của Spark GỒM chính giao dịch đang tính. Nếu serving
``ZADD`` **sau** khi predict thì::

    thẻ im lặng   train = 1   serve = 0
    thẻ đang burst train = 20  serve = 19

Lệch **mọi** prediction, không chỉ lúc burst — model học "1 = bình thường" nhưng
luôn nhận 0. Script Lua dưới đây ghi trước rồi mới đọc, và cả ba bước nằm trong
một lời gọi nên không có race giữa các request song song.

``ZADD`` với cùng member là idempotent (chỉ cập nhật score), nên score lại cùng một
giao dịch (retry) không nhân đôi.

Đánh đổi phải biết
------------------
Redis vào **critical path**: Redis chết thì không tính được velocity. Bắt buộc có
chính sách tường minh (``on_error``) — không được im lặng trả 0, vì 0 nghĩa là
"thẻ này an toàn" và đó là câu trả lời tệ nhất khi ta đang không biết gì.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path

# Độ dài cửa sổ khai ở MỘT chỗ, dùng chung với Spark job (dp3_training_features.py)
# và với test parity. Xem data_pipelines/shared/feature_windows.py.
for _c in (os.environ.get("SHARED_DIR"),
           str(Path(__file__).resolve().parents[3] / "data_pipelines" / "shared")):
    if _c and Path(_c).is_dir():
        sys.path.insert(0, _c)
        break
import feature_windows as W  # noqa: E402

WINDOW_S = W.CARD_VELOCITY_WINDOW_S      # 300
KEY_TTL_S = W.CARD_VELOCITY_KEY_TTL_S    # 600
KEY_PREFIX = "vel:card"

# Ba bước trong MỘT lời gọi -> nguyên tử, không có race giữa request song song.
LUA_CARD_VELOCITY = """
-- KEYS[1] = vel:card:{card_id}
-- ARGV[1] = t  (epoch giây, EVENT TIME của giao dịch)
-- ARGV[2] = window (giây)
-- ARGV[3] = member "txn_id|amount"
-- ARGV[4] = ttl của key (giây)
local t  = tonumber(ARGV[1])
local lo = t - tonumber(ARGV[2])

-- 1) dọn phần đã ra khỏi cửa sổ (đây là thứ làm thẻ im lặng trả về 0)
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', '(' .. lo)

-- 2) GHI giao dịch hiện tại TRƯỚC khi đọc. Bỏ bước này thì lệch 1 đơn vị trên
--    mọi prediction so với rangeBetween(-300, 0) của Spark.
redis.call('ZADD', KEYS[1], t, ARGV[3])
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[4]))

-- 3) đọc trọn cửa sổ [t-window, t]. Bỏ qua entry có score > t (message về muộn
--    xử lý sau một message mới hơn) — Spark cũng chỉ nhìn về phía sau.
local members = redis.call('ZRANGEBYSCORE', KEYS[1], lo, t)
local n, sum = 0, 0.0
for _, m in ipairs(members) do
  n = n + 1
  sum = sum + tonumber(string.sub(m, string.find(m, '|', 1, true) + 1))
end
return {n, tostring(sum)}
"""


@dataclass(frozen=True)
class Velocity:
    """Ba feature velocity 5 phút, tên khớp cột trong ``application.feat_training``."""

    card_tx_count_5min: int
    card_amount_sum_5min: float
    card_amount_avg_5min: float

    def as_request_fields(self) -> dict:
        """Đổi sang tên trường của ``txn_request`` (RequestSource của Feast)."""
        return {
            "card_tx_count_5min_req": self.card_tx_count_5min,
            "card_amount_sum_5min_req": self.card_amount_sum_5min,
            "card_amount_avg_5min_req": self.card_amount_avg_5min,
        }


def member(txn_id: str, amount: float) -> str:
    """Member của sorted set: ``txn_id`` làm khoá dedup, ``amount`` để tính tổng.

    Nhét amount vào member thay vì lưu riêng một hash: một cấu trúc dữ liệu, một
    lần dọn rác, và ``ZADD`` cùng txn_id vẫn idempotent.
    """
    return f"{txn_id}|{amount}"


def key(card_id: str) -> str:
    """Khoá Redis cho một thẻ. Nằm NGOÀI không gian tên của Feast."""
    return f"{KEY_PREFIX}:{card_id}"


def parse(raw: list) -> Velocity:
    """Kết quả thô của script Lua -> ``Velocity``."""
    n = int(raw[0])
    total = float(raw[1])
    return Velocity(n, total, total / n if n else 0.0)


def simulate(txns: list[tuple[str, float, int]], window_s: int = WINDOW_S) -> list[Velocity]:
    """Mô phỏng thuật toán sorted set bằng Python thuần, theo ĐÚNG thứ tự các bước.

    Đây là bản đặc tả có thể chạy được của script Lua: phát lại tuần tự một chuỗi
    ``(txn_id, amount, ts)`` và trả velocity thấy được tại từng giao dịch.

    Dùng bởi ``tests/test_velocity_parity.py`` để đối chiếu với
    ``rangeBetween(-300, 0)`` của Spark mà không cần dựng Spark. Đây là hàng phòng
    ngự duy nhất chống việc hai định nghĩa lệch nhau khi retrain — vì nhóm feature
    này nằm ngoài vòng bảo đảm của Feast.
    """
    zset: dict[str, int] = {}          # member -> score, đúng như một sorted set
    out: list[Velocity] = []
    for txn_id, amount, ts in txns:
        lo = ts - window_s
        # 1) ZREMRANGEBYSCORE -inf (lo
        zset = {m: s for m, s in zset.items() if s >= lo}
        # 2) ZADD (idempotent theo member)
        zset[member(txn_id, amount)] = ts
        # 3) ZRANGEBYSCORE lo ts
        vals = [float(m.rsplit("|", 1)[1]) for m, s in zset.items() if lo <= s <= ts]
        n = len(vals)
        total = float(sum(vals))
        out.append(Velocity(n, total, total / n if n else 0.0))
    return out


class CardVelocity:
    """Bọc script Lua cho một Redis client (async, khớp app đang dùng).

    ``on_error='raise'`` là mặc định CÓ CHỦ Ý. Redis chết là sự cố phải nhìn thấy;
    trả 0 âm thầm nghĩa là nói với model "thẻ này an toàn" đúng lúc ta không biết gì.
    Ai muốn fail-open thì bắt exception ở tầng gọi và đánh dấu ``degraded`` tường minh.
    """

    def __init__(self, redis_client, window_s: int = WINDOW_S,
                 key_ttl_s: int = KEY_TTL_S) -> None:
        self._script = redis_client.register_script(LUA_CARD_VELOCITY)
        self._window_s = window_s
        self._key_ttl_s = key_ttl_s

    async def compute(self, card_id: str, txn_id: str, amount: float,
                      event_ts: float) -> Velocity:
        """1 round-trip (~1ms): dọn -> ghi giao dịch hiện tại -> đọc cả cửa sổ.

        ``event_ts`` là **event time** của giao dịch (epoch giây), không phải
        ``time.time()``: dùng event time thì cửa sổ ở serving trùng đúng cửa sổ
        Spark tính trên cột ``created_at``.
        """
        raw = await self._script(
            keys=[key(card_id)],
            args=[f"{event_ts:.6f}", self._window_s,
                  member(txn_id, amount), self._key_ttl_s],
        )
        return parse(raw)
