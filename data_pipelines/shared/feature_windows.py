"""Định nghĩa CỬA SỔ THỜI GIAN của mọi feature — nguồn duy nhất.

Vì sao cần file này
-------------------
Feast đảm bảo train/serve nhất quán cho những feature đi qua ``FeatureService``.
Nhưng ba nhóm feature trong hệ này được tính bởi **ba công nghệ khác nhau**:

    card_tx_count_5min     Spark (train)  vs  Redis sorted set (serve)
    merch_*_10min          Spark (train)  vs  Flink            (serve)
    device_*_1h            Spark (train)  vs  Flink            (serve)

Nếu mỗi bên tự hardcode độ dài cửa sổ thì một ngày nào đó ai đó đổi serving sang
10 phút mà quên Spark: **không có lỗi nào nổ ra**, chỉ là precision tụt dần sau
mỗi lần retrain và không ai biết tại sao. Nên độ dài cửa sổ khai ở ĐÂY, mọi bên
import về.

Ai import
---------
* Spark job   : ``sys.path.insert(0, "/opt/spark/shared")`` (mount trong compose)
* Flink SQL   : không import được -> giá trị lặp lại trong SQL, và
  ``tests/test_velocity_parity.py`` canh cho khỏi lệch
* API serving : ``src/fraud_detection/features/velocity.py``
* Test parity : ``tests/test_velocity_parity.py``

Module này CỐ TÌNH không import gì (kể cả stdlib) để chạy được ở mọi runtime.
"""

# --------------------------------------------------------------- real-time
# Velocity thẻ — tính ĐỒNG BỘ trong đường score (Redis sorted set).
# Không dùng Flink: card testing là 12-45 giao dịch trong vài giây, mà Flink có
# độ trễ cố hữu ~slide + watermark (~2.5 phút) nên sẽ bỏ sót đúng cái burst.
CARD_VELOCITY_WINDOW_S = 300          # 5 phút
CARD_VELOCITY_KEY_TTL_S = 600         # sorted set tự dọn khi thẻ im lặng

# Feature entity CHIA SẺ — tính bằng Flink (window processing).
# Trễ ~2.5 phút ở đây không sao: tín hiệu "merchant đang bị quét" / "device đang
# bùng nổ" là TRẠNG THÁI kéo dài hàng chục phút, không phải sự kiện tức thời.
MERCHANT_RT_WINDOW_S = 600            # 10 phút
MERCHANT_RT_SLIDE_S = 60              # HOP slide 1 phút
DEVICE_RT_WINDOW_S = 3600             # 1 giờ
DEVICE_RT_SLIDE_S = 300               # HOP slide 5 phút

# Watermark của Flink (phải >= độ trễ tối đa producer tiêm vào: 60s)
FLINK_WATERMARK_S = 90

# Độ trễ thêm của bridge (Kafka poll + feast push) — dùng để suy ngưỡng độ tươi.
BRIDGE_LAG_S = 90

# NGƯỠNG ĐỘ TƯƠI cho giá trị Flink đẩy vào Redis.
#
# Vì sao phải có: Flink KHÔNG phát ra row cho window rỗng. Entity ngừng hoạt động
# -> giá trị cuối cùng ĐÓNG BĂNG trong Redis và entity đó cứ "trông như đang bị
# tấn công" mãi. ODFV so mốc của giá trị với thời điểm giao dịch: quá hạn -> 0.
#
# Ngưỡng phải bằng "TUỔI TỐI ĐA của một giá trị CÒN ĐÚNG", KHÔNG phải "window +
# mọi thứ". Cơ chế:
#
#   Miễn là entity còn hoạt động trong `window` giây gần nhất, các window trượt
#   qua VẪN chứa hoạt động đó nên Flink VẪN emit row mới -> `window_end` luôn tươi,
#   age chỉ ≈ lag (slide + watermark + bridge). Chỉ khi giá trị thật về 0 thì Flink
#   mới ngừng emit và age bắt đầu tăng.
#
# Nên cộng thêm cả `window` vào ngưỡng là tự tạo một CÁI ĐUÔI: sau khi đợt tấn công
# kết thúc, training đã nói 0 (cửa sổ rỗng) mà serving còn nói 13 thêm `window`
# giây nữa. Đuôi đó không phải noise — nó là lệch cỡ chục đơn vị, hình dạng
# false-positive, xảy ra sau MỌI đợt tấn công.
#
#   ngưỡng = slide + watermark + bridge + grace
#
# `grace` là phần duy nhất đặt tay, và nó là một ĐÁNH ĐỔI phải nói rõ:
#
#   * Watermark chỉ tiến khi có message tới. Ở nhịp ~817 giao dịch/ngày (và ban đêm
#     còn thưa hơn ~16 lần) watermark có thể đứng yên vài phút -> Flink emit thưa ->
#     age của giá trị tươi nhất cũng tăng. Grace quá nhỏ sẽ gate BỎ giá trị đúng.
#   * Grace quá lớn thì đuôi false-positive dài ra.
#
# Chọn 180s vì: TRONG lúc bị tấn công, chính merchant/device đó sinh traffic dày
# (card_testing: gap 4-90s) nên watermark tiến đều và age chỉ ~150-250s — tức là
# giá trị lúc CẦN ĐÚNG NHẤT không bao giờ bị gate. Ngoài lúc tấn công, nếu ban đêm
# thưa quá mà giá trị bị gate về 0 thì cũng gần đúng: giá trị thật lúc đó là 0-1.
RT_STALE_GRACE_S = 180

MERCHANT_RT_MAX_AGE_S = (MERCHANT_RT_SLIDE_S + FLINK_WATERMARK_S
                         + BRIDGE_LAG_S + RT_STALE_GRACE_S)       # 420
DEVICE_RT_MAX_AGE_S = (DEVICE_RT_SLIDE_S + FLINK_WATERMARK_S
                       + BRIDGE_LAG_S + RT_STALE_GRACE_S)         # 660

# Ngưỡng phải NHỎ HƠN window, không thì cái đuôi quay lại.
assert MERCHANT_RT_MAX_AGE_S < MERCHANT_RT_WINDOW_S
assert DEVICE_RT_MAX_AGE_S < DEVICE_RT_WINDOW_S

# --------------------------------------------------------------------------- #
# GIAO DỊCH HIỆN TẠI CÓ NẰM TRONG CỬA SỔ HAY KHÔNG
#
# Đây là chi tiết nhỏ nhưng lệch nó thì SAI MỌI PREDICTION, không chỉ lúc burst.
# Hai tầng real-time có ngữ nghĩa khác nhau vì cơ chế serving khác nhau:
#
#   tầng 5 phút (sorted set): API `ZADD` giao dịch hiện tại RỒI mới đọc
#       -> serving CÓ tính nó  -> Spark phải dùng rangeBetween(-w, 0)
#
#   tầng Flink (10'/1h): giá trị trong Redis là của window đã chốt TRƯỚC khi giao
#       dịch hiện tại tới, nên Flink KHÔNG thể tính nó
#       -> serving KHÔNG tính  -> Spark phải dùng rangeBetween(-w, -1)
#
# Nếu để cả hai là (-w, 0): merchant im lặng (đa số) sẽ có train=1 vs serve=0.
# Nếu để cả hai là (-w, -1): thẻ im lặng sẽ có train=0 vs serve=1.
# Cả hai trường hợp đều là lệch hệ thống trên ~99% số dòng.
SELF_INCLUSIVE_TIERS = ("card_velocity",)          # rangeBetween(-w, 0)
SELF_EXCLUSIVE_TIERS = ("merchant_rt", "device_rt")  # rangeBetween(-w, -1)

# --------------------------------------------------------------------- batch
# Cửa sổ dài, Spark tính hằng ngày. Dài hơn nhịp batch (24h) rất nhiều nên
# materialize từ Postgres lên Redis là hợp lệ (chậm 1 ngày = <=3% cửa sổ).
CARD_SHORT_WINDOW_D = 7               # ghép với 90d -> tỉ lệ tăng tốc (bust-out)
CARD_LONG_WINDOW_D = 90
USER_WINDOW_D = 30
DEVICE_WINDOW_D = 30
MERCHANT_WINDOW_D = 30

# Số ngày trong cửa sổ dài của thẻ, dùng để chuẩn hoá card_acceleration:
# nhịp giao dịch 7 ngày so với nhịp trung bình 90 ngày quy về cùng đơn vị.
CARD_ACCEL_RATIO = CARD_LONG_WINDOW_D / CARD_SHORT_WINDOW_D   # ~12.857

# --------------------------------------------------------------------------- #
# AI ĐƯỢC MATERIALIZE — nguồn duy nhất cho câu hỏi này.
#
# Khai ở đây (module không phụ thuộc gì) để CẢ BA nơi đọc cùng một danh sách: DAG
# Airflow khi dựng lệnh ``feast materialize``, ``feature_store/feature_views.py``,
# và docs. Hardcode trong DAG thì thêm một view mới mà quên sửa DAG sẽ làm nó âm
# thầm không được materialize — hoặc tệ hơn, được materialize dù không nên.
#
# Vì sao PUSH_VIEWS không được materialize: cửa sổ 10 phút / 1 giờ NGẮN HƠN nhịp
# batch (24h) nên giá trị batch chậm tối đa 24 giờ — không phải "hơi cũ" mà là vô
# nghĩa. Ví dụ 14:00 merchant đang bị quét thẻ: Flink đẩy 25 (đúng), bảng batch từ
# 00:15 có 1 (giao dịch cuối hôm qua). Materialize ghi 1 lên 25, và vì
# ``_ts:{view}`` cũng bị ghi đè nên giá trị sai đó Ở LẠI tới lần push kế tiếp.
BATCH_VIEWS = ["card_features", "user_features", "merchant_features", "device_features"]
PUSH_VIEWS = ["merchant_realtime", "device_realtime"]

# --------------------------------------------------------------------- tiện
DAY_S = 24 * 3600
CARD_LONG_WINDOW_S = CARD_LONG_WINDOW_D * DAY_S
CARD_SHORT_WINDOW_S = CARD_SHORT_WINDOW_D * DAY_S
USER_WINDOW_S = USER_WINDOW_D * DAY_S
DEVICE_WINDOW_S = DEVICE_WINDOW_D * DAY_S
MERCHANT_WINDOW_S = MERCHANT_WINDOW_D * DAY_S

# Timezone nghiệp vụ — dùng cho is_night / event_date / partition.
LOCAL_TZ = "Asia/Ho_Chi_Minh"
