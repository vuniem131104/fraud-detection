"""Feast feature views — ba tầng feature của hệ fraud detection.

    TẦNG BATCH        DP3a (Spark) -> Postgres -> feast materialize -> Redis
      card_features / user_features / merchant_features / device_features
      cửa sổ 7d / 30d / 90d — dài hơn nhịp batch (24h) rất nhiều nên chậm 1 ngày
      chỉ là <=3% cửa sổ => materialize hợp lệ

    TẦNG STREAMING    Flink -> Kafka -> bridge push -> Redis
      merchant_realtime (10 phút) / device_realtime (1 giờ)
      cửa sổ NGẮN HƠN nhịp batch => giá trị batch luôn sai => TUYỆT ĐỐI KHÔNG
      materialize (xem PUSH_VIEWS bên dưới)

    TẦNG ĐỒNG BỘ      code API (Redis sorted set) -> RequestSource
      card_tx_count_5min / card_amount_sum_5min / card_amount_avg_5min
      cần chính xác từng giây và phải thấy giao dịch ngay trước đó, nên Flink
      (trễ ~2,5 phút) không dùng được. Xem src/fraud_detection/features/velocity.py

    ON-DEMAND         txn_on_demand — tính tại request time
      tỉ lệ so với baseline, tuổi entity, kiểm tra độ tươi giá trị Flink

Nguyên tắc thiết kế xuyên suốt: **bảng batch chỉ chứa baseline, ODFV tính tỉ lệ.**
``amount_usd = 500`` chẳng nói gì; ``500`` trên thẻ có ``card_amount_avg_90d = 12``
thì nói rất nhiều. Nhờ vậy công thức tỉ lệ chỉ tồn tại MỘT chỗ và chạy y hệt ở cả
train lẫn serve.

``ttl=timedelta(0)`` = vô hạn: feature ở lại online store tới khi bị ghi đè.
"""

import os
import sys
from datetime import timedelta
from pathlib import Path

import pandas as pd
from feast import Field, FeatureView, RequestSource
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Bool, Float64, Int64, String, UnixTimestamp

from data_sources import (
    card_source,
    device_rt_push_source,
    device_source,
    merchant_rt_push_source,
    merchant_source,
    user_source,
)
from entities import card, device, merchant, user

# Độ dài cửa sổ + ngưỡng độ tươi khai ở MỘT chỗ, dùng chung với Spark job.
for _c in (os.environ.get("SHARED_DIR"),
           str(Path(__file__).resolve().parents[1] / "data_pipelines" / "shared")):
    if _c and Path(_c).is_dir():
        sys.path.insert(0, _c)
        break
import feature_windows as W  # noqa: E402

# ===================================================================== BATCH
card_features = FeatureView(
    name="card_features",
    entities=[card],
    ttl=timedelta(0),
    source=card_source,
    schema=[
        # thuộc tính dim (gộp cùng bảng để serving chỉ cần 1 lượt tra Redis)
        Field(name="card_brand", dtype=String),
        Field(name="card_type", dtype=String),
        Field(name="is_virtual", dtype=Bool),
        Field(name="card_created_at", dtype=UnixTimestamp),
        # baseline 90 ngày — mẫu số cho mọi tỉ lệ về số tiền
        Field(name="card_tx_count_90d", dtype=Int64),
        Field(name="card_amount_sum_90d", dtype=Float64),
        Field(name="card_amount_avg_90d", dtype=Float64),
        Field(name="card_amount_max_90d", dtype=Float64),
        Field(name="card_amount_std_90d", dtype=Float64),
        Field(name="card_distinct_merchant_90d", dtype=Int64),
        # nhịp 7 ngày — ghép với 90d thành card_acceleration (bắt bust-out)
        Field(name="card_tx_count_7d", dtype=Int64),
        # giao dịch gần nhất — cho hours_since_last_card_tx (dormant -> active)
        Field(name="card_last_tx_at", dtype=UnixTimestamp),
    ],
)

user_features = FeatureView(
    name="user_features",
    entities=[user],
    ttl=timedelta(0),
    source=user_source,
    schema=[
        Field(name="customer_segment", dtype=String),
        Field(name="kyc_level", dtype=Int64),
        Field(name="email_verified", dtype=Bool),
        Field(name="user_country", dtype=String),
        Field(name="account_created_at", dtype=UnixTimestamp),
        Field(name="user_tx_count_30d", dtype=Int64),
        Field(name="user_amount_avg_30d", dtype=Float64),
        Field(name="user_device_count_30d", dtype=Int64),
        # "user này bình thường giao dịch từ mấy nước" -> mẫu số cho geo_mismatch:
        # người hay đi công tác 4 nước thì lệch quốc gia không đáng lo
        Field(name="user_distinct_country_30d", dtype=Int64),
        Field(name="user_last_tx_at", dtype=UnixTimestamp),
    ],
)

merchant_features = FeatureView(
    name="merchant_features",
    entities=[merchant],
    ttl=timedelta(0),
    source=merchant_source,
    schema=[
        Field(name="merchant_category", dtype=String),
        Field(name="merchant_risk_level", dtype=Int64),
        Field(name="merchant_tx_count_30d", dtype=Int64),
        # avg + std -> z-score số tiền so với CHUẨN CỦA MERCHANT. 3$ ở merchant
        # trung bình 80$ = đang thử thẻ. Một trong những feature rẻ mà mạnh nhất.
        Field(name="merchant_amount_avg_30d", dtype=Float64),
        Field(name="merchant_amount_std_30d", dtype=Float64),
        Field(name="merchant_distinct_cards_30d", dtype=Int64),
    ],
)

device_features = FeatureView(
    name="device_features",
    entities=[device],
    ttl=timedelta(0),
    source=device_source,
    schema=[
        Field(name="device_tx_count_30d", dtype=Int64),
        # graph: 1 device bị bao nhiêu user dùng -> bắt fraud ring
        Field(name="device_distinct_users_30d", dtype=Int64),
        # distinct CARDS mạnh hơn distinct users: device farm quay vòng 40 thẻ trộm
        # nhưng có thể chỉ dựng 3-4 "user"
        Field(name="device_distinct_cards_30d", dtype=Int64),
        Field(name="device_first_seen_at", dtype=UnixTimestamp),
    ],
)

# ================================================================= STREAMING
# Flink tính -> bridge push vào Redis. KHÔNG BAO GIỜ materialize (xem PUSH_VIEWS).
#
# Field mang tiền tố ``raw_``: Feast không filter online read theo ttl nên giá trị
# đọc ra có thể đã quá hạn. Flink KHÔNG phát ra row cho window rỗng, nên merchant
# ngừng hoạt động thì giá trị cuối cùng ĐÓNG BĂNG trong Redis và merchant đó cứ
# "trông như đang bị quét thẻ" mãi. ODFV so ``*_ts_epoch`` với thời điểm giao dịch
# rồi mới xuất ra tên chuẩn -> model không bao giờ thấy giá trị chưa kiểm tra.
merchant_realtime = FeatureView(
    name="merchant_realtime",
    entities=[merchant],
    ttl=timedelta(seconds=W.MERCHANT_RT_MAX_AGE_S),
    source=merchant_rt_push_source,
    schema=[
        Field(name="raw_merch_tx_count_10min", dtype=Int64),
        Field(name="raw_merch_distinct_cards_10min", dtype=Int64),
        Field(name="raw_merch_amount_avg_10min", dtype=Float64),
        Field(name="merchant_rt_ts_epoch", dtype=Int64),
    ],
)

device_realtime = FeatureView(
    name="device_realtime",
    entities=[device],
    ttl=timedelta(seconds=W.DEVICE_RT_MAX_AGE_S),
    source=device_rt_push_source,
    schema=[
        Field(name="raw_device_tx_count_1h", dtype=Int64),
        Field(name="raw_device_distinct_users_1h", dtype=Int64),
        Field(name="raw_device_distinct_cards_1h", dtype=Int64),
        Field(name="device_rt_ts_epoch", dtype=Int64),
    ],
)

# ==================================================================== REQUEST
# Dữ liệu của chính giao dịch đang được chấm điểm.
#
# Ba trường ``*_5min_req`` là velocity do **code API** tính bằng Redis sorted set
# ngay trong đường score (xem src/fraud_detection/features/velocity.py). Chúng đi
# vào Feast qua RequestSource thay vì FeatureView vì không có tiến trình nào ghi
# chúng trước: chúng được tạo ra tại chính lời gọi score. Lúc training, notebook
# lấy đúng ba cột đó từ ``feat_training`` (Spark tính bằng cửa sổ y hệt) -> ODFV
# nhận đầu vào giống nhau ở cả hai bên.
txn_request = RequestSource(
    name="txn_request",
    schema=[
        Field(name="amount_usd", dtype=Float64),
        Field(name="billing_country_code", dtype=String),
        Field(name="ip_country_code", dtype=String),
        Field(name="email_purchaser", dtype=String),
        Field(name="email_recipient", dtype=String),
        Field(name="event_ts_epoch", dtype=Int64),
        Field(name="card_tx_count_5min_req", dtype=Int64),
        Field(name="card_amount_sum_5min_req", dtype=Float64),
        Field(name="card_amount_avg_5min_req", dtype=Float64),
    ],
)

_EPS = 1e-9


def _safe_div(num, den, floor: float = 1.0):
    """Chia có sàn cho mẫu số — entity mới có baseline 0, không được thành inf."""
    return num.astype("float64") / den.astype("float64").clip(lower=floor)


@on_demand_feature_view(
    sources=[txn_request, card_features, user_features, merchant_features,
             device_features, merchant_realtime, device_realtime],
    schema=[
        # --- thời gian / số tiền thô ---
        Field(name="log_amount", dtype=Float64),
        Field(name="hour", dtype=Int64),
        Field(name="weekday", dtype=Int64),
        Field(name="is_night", dtype=Int64),
        # --- danh tính / địa lý ---
        Field(name="geo_mismatch", dtype=Int64),
        Field(name="foreign_ip", dtype=Int64),
        Field(name="recipient_differs", dtype=Int64),
        # --- tuổi entity (đổi mỗi giây -> không lưu sẵn) ---
        Field(name="account_age_days", dtype=Int64),
        Field(name="card_age_days", dtype=Int64),
        Field(name="device_age_hours", dtype=Float64),
        # --- số tiền so với baseline của chính entity ---
        Field(name="amount_vs_card_avg", dtype=Float64),
        Field(name="amount_vs_card_max", dtype=Float64),
        Field(name="amount_z_vs_card", dtype=Float64),
        Field(name="amount_vs_user_avg", dtype=Float64),
        Field(name="amount_z_vs_merchant", dtype=Float64),
        # --- nhịp độ / ngủ đông ---
        Field(name="card_acceleration", dtype=Float64),
        Field(name="hours_since_last_card_tx", dtype=Float64),
        Field(name="hours_since_last_user_tx", dtype=Float64),
        # --- rủi ro merchant (cần Flink) ---
        Field(name="merch_tx_count_10min", dtype=Int64),
        Field(name="merch_distinct_cards_10min", dtype=Int64),
        Field(name="merch_amount_avg_10min", dtype=Float64),
        Field(name="merchant_spread", dtype=Float64),
        Field(name="merchant_burst", dtype=Float64),
        # --- fraud ring (cần Flink) ---
        Field(name="device_tx_count_1h", dtype=Int64),
        Field(name="device_distinct_users_1h", dtype=Int64),
        Field(name="device_distinct_cards_1h", dtype=Int64),
        Field(name="device_burst", dtype=Float64),
        Field(name="device_cards_per_user", dtype=Float64),
        # --- velocity 5 phút (API tính, đưa vào hợp đồng feature) ---
        Field(name="card_tx_count_5min", dtype=Int64),
        Field(name="card_amount_sum_5min", dtype=Float64),
        Field(name="card_amount_avg_5min", dtype=Float64),
    ],
)
def txn_on_demand(inp: pd.DataFrame) -> pd.DataFrame:
    """Feature tính tại request time từ request + các view đã tra được.

    Cùng một hàm chạy cả lúc train (trên dòng lịch sử của ``feat_training``) lẫn
    lúc serve (trên request) -> không thể lệch định nghĩa giữa hai bên.
    """
    import numpy as np

    out = pd.DataFrame(index=inp.index)

    # ts_utc để trừ ngày; ts_local để lấy giờ trong ngày (is_night mới có nghĩa)
    ts_utc = pd.to_datetime(inp["event_ts_epoch"], unit="s", utc=True)
    ts_local = ts_utc.dt.tz_convert(W.LOCAL_TZ)
    amount = inp["amount_usd"].astype("float64")

    # ------------------------------------------------- thời gian / số tiền
    out["log_amount"] = np.log1p(amount)
    out["hour"] = ts_local.dt.hour.astype("int64")
    out["weekday"] = ts_local.dt.weekday.astype("int64")
    out["is_night"] = ((ts_local.dt.hour < 6) | (ts_local.dt.hour >= 22)).astype("int64")

    # ------------------------------------------------- danh tính / địa lý
    out["geo_mismatch"] = (
        inp["billing_country_code"] != inp["ip_country_code"]).astype("int64")
    out["foreign_ip"] = (
        inp["ip_country_code"] != inp["user_country"]).astype("int64")
    out["recipient_differs"] = (
        inp["email_recipient"].fillna("") != inp["email_purchaser"].fillna("")
    ).astype("int64")

    # ------------------------------------------------- tuổi entity
    out["account_age_days"] = (
        (ts_utc - pd.to_datetime(inp["account_created_at"], utc=True)).dt.days
        .fillna(-1).astype("int64"))
    out["card_age_days"] = (
        (ts_utc - pd.to_datetime(inp["card_created_at"], utc=True)).dt.days
        .fillna(-1).astype("int64"))
    # device chưa từng thấy -> null. Trả 0 giờ ("vừa mới xuất hiện") thay vì -1:
    # đó đúng là ý nghĩa nghiệp vụ, và là cờ đỏ mạnh cho account takeover.
    dev_first = pd.to_datetime(inp["device_first_seen_at"], utc=True)
    out["device_age_hours"] = (
        ((ts_utc - dev_first).dt.total_seconds() / 3600.0)
        .fillna(0.0).clip(lower=0.0).astype("float64"))

    # ------------------------------------- số tiền so với baseline entity
    card_avg = inp["card_amount_avg_90d"].fillna(0.0)
    card_max = inp["card_amount_max_90d"].fillna(0.0)
    card_std = inp["card_amount_std_90d"].fillna(0.0)
    out["amount_vs_card_avg"] = _safe_div(amount, card_avg)
    # > 1 nghĩa là vượt kỷ lục chi tiêu của thẻ — mạnh hơn "lớn hơn trung bình"
    out["amount_vs_card_max"] = _safe_div(amount, card_max)
    out["amount_z_vs_card"] = (amount - card_avg) / card_std.clip(lower=1.0)
    out["amount_vs_user_avg"] = _safe_div(amount, inp["user_amount_avg_30d"].fillna(0.0))
    # z-score so với chuẩn của MERCHANT: 3$ ở merchant trung bình 80$ = thử thẻ
    merch_avg = inp["merchant_amount_avg_30d"].fillna(0.0)
    merch_std = inp["merchant_amount_std_30d"].fillna(0.0)
    out["amount_z_vs_merchant"] = (amount - merch_avg) / merch_std.clip(lower=1.0)

    # ------------------------------------------------- nhịp độ / ngủ đông
    # nhịp 7 ngày so với nhịp trung bình 90 ngày, quy về cùng đơn vị.
    # > 1 = thẻ đang tăng tốc (dấu hiệu bust-out).
    baseline_7d = inp["card_tx_count_90d"].fillna(0.0) / W.CARD_ACCEL_RATIO
    out["card_acceleration"] = _safe_div(inp["card_tx_count_7d"].fillna(0.0), baseline_7d)
    for name, col in (("hours_since_last_card_tx", "card_last_tx_at"),
                      ("hours_since_last_user_tx", "user_last_tx_at")):
        last = pd.to_datetime(inp[col], utc=True)
        # chưa từng giao dịch -> -1 (khác hẳn "vừa giao dịch xong")
        out[name] = (((ts_utc - last).dt.total_seconds() / 3600.0)
                     .fillna(-1.0).astype("float64"))

    # ------------------------------- Flink: kiểm tra ĐỘ TƯƠI rồi mới dùng
    # Flink không emit row cho window rỗng -> giá trị cuối bị đóng băng trong Redis.
    # So mốc của giá trị với thời điểm giao dịch; quá hạn thì coi như 0 (đúng nghĩa
    # "không có hoạt động gần đây"). Cùng phép kiểm tra này chạy ở train và serve.
    def _fresh(ts_col: str, max_age: int):
        age = (inp["event_ts_epoch"].astype("float64")
               - inp[ts_col].astype("float64"))
        # biên -60s: message về muộn có thể cho window_end hơi vượt event time
        return age.notna() & (age >= -60) & (age <= max_age)

    m_fresh = _fresh("merchant_rt_ts_epoch", W.MERCHANT_RT_MAX_AGE_S)
    d_fresh = _fresh("device_rt_ts_epoch", W.DEVICE_RT_MAX_AGE_S)

    def _gated(mask, col: str, dtype: str):
        zero = 0 if dtype == "int64" else 0.0
        return np.where(mask, inp[col].fillna(zero), zero).astype(dtype)

    out["merch_tx_count_10min"] = _gated(m_fresh, "raw_merch_tx_count_10min", "int64")
    out["merch_distinct_cards_10min"] = _gated(
        m_fresh, "raw_merch_distinct_cards_10min", "int64")
    out["merch_amount_avg_10min"] = _gated(
        m_fresh, "raw_merch_amount_avg_10min", "float64")
    out["device_tx_count_1h"] = _gated(d_fresh, "raw_device_tx_count_1h", "int64")
    out["device_distinct_users_1h"] = _gated(
        d_fresh, "raw_device_distinct_users_1h", "int64")
    out["device_distinct_cards_1h"] = _gated(
        d_fresh, "raw_device_distinct_cards_1h", "int64")

    # ------------------------------- cặp ngắn+dài = feature mạnh nhất của bộ
    # Số giao dịch trên mỗi thẻ trong cửa sổ. Bình thường = 1 (một khách quẹt một
    # lần). Cao = MỘT thẻ bị quẹt lặp lại ở một merchant -> đúng dấu vết
    # card_testing của hệ này (đo được: cửa sổ 10 phút có >=5 giao dịch thì 92%
    # là fraud, trong khi nền chỉ 0,3%).
    # Lưu ý: kiểu tấn công còn lại — bot quét NHIỀU thẻ, mỗi thẻ một lần — cho
    # spread ≈ 1 giống khách thật, nên phải đọc CÙNG merch_tx_count_10min mới
    # phân biệt được. Hai feature này chỉ có nghĩa khi đi cặp.
    out["merchant_spread"] = _safe_div(
        out["merch_tx_count_10min"], out["merch_distinct_cards_10min"])
    # merchant đang lệch khỏi chuẩn của CHÍNH NÓ (0,04 = toàn giao dịch 3$/80$)
    out["merchant_burst"] = _safe_div(out["merch_amount_avg_10min"], merch_avg)
    # device đang bùng nổ so với nền 30 ngày của nó
    dev_users_30 = inp["device_distinct_users_30d"].fillna(0.0)
    out["device_burst"] = _safe_div(out["device_distinct_users_1h"], dev_users_30)
    out["device_cards_per_user"] = _safe_div(
        inp["device_distinct_cards_30d"].fillna(0.0), dev_users_30)

    # ------------------------------- velocity 5 phút (API tính bằng sorted set)
    # Đi thẳng vào hợp đồng feature: không cần kiểm tra độ tươi vì sorted set tự
    # xoá phần ngoài cửa sổ (ZREMRANGEBYSCORE) -> thẻ im lặng trả 0 một cách tự
    # nhiên, không bao giờ đóng băng.
    out["card_tx_count_5min"] = inp["card_tx_count_5min_req"].fillna(0).astype("int64")
    out["card_amount_sum_5min"] = (
        inp["card_amount_sum_5min_req"].fillna(0.0).astype("float64"))
    out["card_amount_avg_5min"] = (
        inp["card_amount_avg_5min_req"].fillna(0.0).astype("float64"))
    return out


# ===================================================================== LUẬT
# "View nào được materialize" khai ở ``shared/feature_windows.py`` — module không
# phụ thuộc gì, nên DAG Airflow đọc được mà không phải import cả Feast. Re-export
# ở đây để đọc code feature store là thấy luôn.
#
# Kiểm tra nhất quán ngay lúc ``feast apply``: sai tên view thì lỗi ở đây, không
# phải im lặng lúc materialize.
BATCH_VIEWS = W.BATCH_VIEWS
PUSH_VIEWS = W.PUSH_VIEWS

_declared = {v.name for v in (card_features, user_features, merchant_features,
                              device_features, merchant_realtime, device_realtime)}
_listed = set(BATCH_VIEWS) | set(PUSH_VIEWS)
assert _listed == _declared, (
    "BATCH_VIEWS + PUSH_VIEWS phải phủ đúng các FeatureView đã khai. "
    f"Chỉ có trong danh sách: {sorted(_listed - _declared)}. "
    f"Chỉ có trong code: {sorted(_declared - _listed)}.")
