"""Canh cho HAI cài đặt của velocity 5 phút không lệch nhau.

Vì sao cần test này
-------------------
``card_tx_count_5min`` là feature duy nhất trong hệ được tính bởi hai công nghệ
khác nhau và **nằm ngoài vòng bảo đảm của Feast**:

    training : Spark  ``Window.rangeBetween(-300, 0)``   (dp3_training_features.py)
    serving  : Redis  sorted set trong đường score       (features/velocity.py)

Nếu sáu tháng sau ai đó đổi serving sang 10 phút mà quên Spark thì **không có lỗi
nào nổ ra**: metric offline vẫn đẹp (train/test cùng định nghĩa Spark), chỉ có
production tệ dần sau mỗi lần retrain và không ai biết tại sao. Test này là hàng
phòng ngự duy nhất.

Nó kiểm hai điều
----------------
1. Thuật toán sorted set (``velocity.simulate``, bản đặc tả chạy được của script
   Lua) cho ra ĐÚNG kết quả của ``rangeBetween(-300, 0)``.
2. Script Lua thật chạy trên Redis khớp với bản mô phỏng đó — chỉ chạy khi có
   Redis (``REDIS_HOST``), ngược lại skip.

Chạy::

    uv run pytest tests/test_velocity_parity.py -v
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(REPO_ROOT / "data_pipelines" / "shared"))

import feature_windows as W  # noqa: E402
from fraud_detection.features import velocity  # noqa: E402

WINDOW = W.CARD_VELOCITY_WINDOW_S


# --------------------------------------------------------------------------- #
# Bản tham chiếu của ngữ nghĩa Spark                                           #
# --------------------------------------------------------------------------- #

def spark_range_between(txns: list[tuple[str, float, int]], window_s: int = WINDOW):
    """Bản Python thuần của ``Window.partitionBy(card).orderBy(ts).rangeBetween(-w, 0)``.

    Với mỗi giao dịch i: lấy mọi giao dịch j có ``ts_i - w <= ts_j <= ts_i``.
    Biên phải là ``<=`` (GỒM chính giao dịch i) — đây là điểm quan trọng nhất.
    """
    out = []
    for _tid_i, _amt_i, ts_i in txns:
        lo = ts_i - window_s
        vals = [a for _tid, a, ts in txns if lo <= ts <= ts_i]
        n = len(vals)
        total = float(sum(vals))
        out.append(velocity.Velocity(n, total, total / n if n else 0.0))
    return out


def assert_same(got, want, label: str) -> None:
    """So từng dòng, báo rõ dòng nào lệch."""
    assert len(got) == len(want), f"{label}: số dòng lệch"
    for i, (g, w) in enumerate(zip(got, want)):
        assert g.card_tx_count_5min == w.card_tx_count_5min, (
            f"{label}: dòng {i} count {g.card_tx_count_5min} != {w.card_tx_count_5min}")
        assert g.card_amount_sum_5min == pytest.approx(w.card_amount_sum_5min), (
            f"{label}: dòng {i} sum {g.card_amount_sum_5min} != {w.card_amount_sum_5min}")
        assert g.card_amount_avg_5min == pytest.approx(w.card_amount_avg_5min), (
            f"{label}: dòng {i} avg {g.card_amount_avg_5min} != {w.card_amount_avg_5min}")


# --------------------------------------------------------------------------- #
# Bộ dữ liệu                                                                   #
# --------------------------------------------------------------------------- #

def _seq(gaps_s: list[float], amount: float = 10.0, t0: int = 1_800_000_000):
    """Chuỗi giao dịch từ danh sách khoảng cách (giây) giữa các lần liên tiếp.

    Mốc thời gian là float — khớp thực tế: ``created_at`` có độ phân giải
    microsecond, và cả Spark (``cast("double")``) lẫn Lua (``%.6f``) đều giữ nó.
    """
    out, ts = [], float(t0)
    for i, gap in enumerate(gaps_s):
        ts += gap
        out.append((f"tx{i:03d}", amount, ts))
    return out


CASES = {
    # thẻ bình thường: mỗi giao dịch cách nhau nhiều hơn cửa sổ -> luôn đúng 1
    "thẻ_im_lặng": _seq([0] + [WINDOW + 60] * 5),
    # card testing: 30 giao dịch cách nhau 10s -> đếm tăng dần rồi bão hoà
    "card_testing": _seq([0] + [10] * 29, amount=3.4),
    # ranh giới cửa sổ: đúng 300s và 301s
    "biên_cửa_sổ": _seq([0, WINDOW, 1, WINDOW - 1]),
    # nhiều giao dịch trong CÙNG một giây, khác nhau ở microsecond (case thật)
    "trong_cùng_giây": _seq([0, 0.15, 0.3, 0.55, 5]),
    # burst rồi im lặng: giá trị phải TỤT về 1, không đóng băng
    "burst_rồi_im": _seq([0] + [5] * 10 + [WINDOW * 2] + [5] * 3),
    # số tiền khác nhau -> kiểm cả sum/avg, không chỉ count
    "số_tiền_lệch": [("a", 0.5, 1_800_000_000.0), ("b", 999.0, 1_800_000_010.5),
                     ("c", 3.25, 1_800_000_020.25)],
}


@pytest.mark.parametrize("name", list(CASES))
def test_sorted_set_matches_spark_window(name: str) -> None:
    """Thuật toán sorted set == rangeBetween(-300, 0) trên mọi bộ dữ liệu."""
    txns = CASES[name]
    assert_same(velocity.simulate(txns, WINDOW), spark_range_between(txns, WINDOW), name)


def test_current_transaction_is_included() -> None:
    """Thẻ chưa từng giao dịch phải ra 1, KHÔNG phải 0.

    Đây là bug off-by-one nguy hiểm nhất: nếu serving ``ZADD`` sau khi predict thì
    mọi thẻ im lặng trả 0 trong khi model được train trên giá trị 1.
    """
    (first,) = velocity.simulate([("tx0", 12.5, 1_800_000_000)], WINDOW)
    assert first.card_tx_count_5min == 1
    assert first.card_amount_sum_5min == pytest.approx(12.5)
    assert first.card_amount_avg_5min == pytest.approx(12.5)


def test_duplicate_txn_id_does_not_double_count() -> None:
    """Gửi lại cùng ``txn_id`` (retry / at-least-once) không làm count tăng.

    Khớp với DP2: Spark ``dropDuplicates(["id"])`` cũng khử theo id. Hai cơ chế
    khác nhau nhưng cùng kết quả.
    """
    t = 1_800_000_000
    got = velocity.simulate([("tx0", 10.0, t), ("tx0", 10.0, t), ("tx1", 20.0, t + 1)],
                            WINDOW)
    assert [v.card_tx_count_5min for v in got] == [1, 1, 2]
    assert got[-1].card_amount_sum_5min == pytest.approx(30.0)


def test_exact_ties_are_known_divergence() -> None:
    """Hai giao dịch có mốc thời gian TRÙNG KHÍT là điểm duy nhất hai bên khác nhau.

    ``rangeBetween`` so theo GIÁ TRỊ cột order, nên nếu hai giao dịch cùng mốc thì
    mỗi cái đều nằm trong cửa sổ của cái kia — kể cả cái xảy ra SAU. Đó là rò rỉ
    tương lai vào quá khứ, và **Spark là bên sai**: sorted set chỉ thấy giao dịch đã
    tới, đúng nhân quả.

    Trong hệ này trùng khít gần như không xảy ra vì ``created_at`` có độ phân giải
    microsecond và job Spark dùng ``cast("double")`` (không làm tròn về giây). Test
    này ghi lại hành vi để không ai "sửa" sorted set theo Spark.
    """
    t = 1_800_000_000.0
    txns = [("tx0", 10.0, t), ("tx1", 10.0, t), ("tx2", 10.0, t)]

    serving = [v.card_tx_count_5min for v in velocity.simulate(txns, WINDOW)]
    spark = [v.card_tx_count_5min for v in spark_range_between(txns, WINDOW)]

    assert serving == [1, 2, 3], "serving phải chỉ thấy giao dịch đã tới"
    assert spark == [3, 3, 3], "rangeBetween thấy cả giao dịch xảy ra sau (rò rỉ)"

    # Lệch biến mất ngay khi mốc thời gian phân biệt được ở mức microsecond.
    micro = [("tx0", 10.0, t), ("tx1", 10.0, t + 1e-6), ("tx2", 10.0, t + 2e-6)]
    assert_same(velocity.simulate(micro, WINDOW),
                spark_range_between(micro, WINDOW), "microsecond")


def test_window_length_comes_from_shared_module() -> None:
    """Serving và Spark phải đọc cùng một hằng số, không hardcode riêng."""
    assert velocity.WINDOW_S == W.CARD_VELOCITY_WINDOW_S == 300
    assert velocity.KEY_TTL_S == W.CARD_VELOCITY_KEY_TTL_S
    # TTL phải LỚN HƠN cửa sổ, không thì entry bị xoá trước khi hết hiệu lực
    assert velocity.KEY_TTL_S > velocity.WINDOW_S


# --------------------------------------------------------------------------- #
# Script Lua thật (cần Redis)                                                  #
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("name", list(CASES))
def test_lua_matches_simulation(name: str) -> None:
    """Script Lua chạy trên Redis thật == bản mô phỏng Python.

    Skip nếu không có Redis: test ở trên đã canh phần ngữ nghĩa, test này canh
    phần *cài đặt* (thứ tự lệnh, biên inclusive/exclusive của ZRANGEBYSCORE).
    """
    redis = pytest.importorskip("redis", reason="cần redis-py")
    host = os.environ.get("REDIS_HOST", "localhost")
    port = int(os.environ.get("REDIS_PORT", "6379"))
    client = redis.Redis(host=host, port=port, socket_connect_timeout=1)
    try:
        client.ping()
    except Exception as exc:                    # noqa: BLE001 - môi trường, không phải lỗi test
        pytest.skip(f"không có Redis ở {host}:{port} ({exc})")

    txns = CASES[name]
    card_id = f"parity-{name}"
    client.delete(velocity.key(card_id))
    script = client.register_script(velocity.LUA_CARD_VELOCITY)
    got = []
    for txn_id, amount, ts in txns:
        raw = script(keys=[velocity.key(card_id)],
                     args=[f"{ts:.6f}", WINDOW, velocity.member(txn_id, amount),
                           velocity.KEY_TTL_S])
        got.append(velocity.parse(raw))
    client.delete(velocity.key(card_id))
    assert_same(got, velocity.simulate(txns, WINDOW), f"lua:{name}")
