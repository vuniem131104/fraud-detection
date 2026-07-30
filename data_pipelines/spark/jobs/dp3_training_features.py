"""DP3b — Gold -> ``application.feat_training``: feature POINT-IN-TIME cho training.

Vì sao cần job riêng, không dùng lại ``feat_*``
----------------------------------------------
``dp3_gold_to_features.py`` tạo **snapshot as-of hôm nay**: mỗi entity đúng 1 dòng.
Đó là thứ *serving* cần. Nhưng join snapshot đó vào một giao dịch tháng 9/2025 sẽ
gán cho nó số của tháng 7/2026 — sai thời điểm, và rò rỉ tương lai vào quá khứ.

Job này tính **cùng những công thức đó** nhưng neo vào từng giao dịch::

    snapshot (serving):  agg(cửa sổ kết thúc tại HÔM NAY)
    PIT      (training): agg(cửa sổ kết thúc tại THỜI ĐIỂM GIAO DỊCH)

Cùng định nghĩa, khác mốc neo -> không sinh train/serve skew.

Đây là bảng offline DUY NHẤT cho training: nó chứa cả ba tầng feature, kể cả hai
tầng mà lúc serve do công nghệ khác tính::

    tầng batch      7d/30d/90d       serve: Redis (feast materialize)
    tầng Flink      10 phút / 1 giờ  serve: Flink push -> Redis
    tầng đồng bộ    5 phút           serve: Redis sorted set trong đường score

Hai khe hở đã biết (ghi rõ để không ai tưởng là bug)
----------------------------------------------------
1. **Cửa sổ 10 phút / 1 giờ**: ở đây là rolling chính xác ``[t-w, t]``; Flink dùng
   HOP căn theo lưới + watermark 90s. Chấp nhận được vì tín hiệu merchant/device
   thay đổi chậm (trạng thái kéo dài hàng chục phút) nên lệch vài phút không đổi
   kết luận. Mô phỏng đúng lưới của Flink trong Spark tốn gấp mấy lần code mà lợi
   ích gần bằng 0.
2. **``card_last_tx_at``**: ở đây là giao dịch NGAY TRƯỚC; lúc serve giá trị đến từ
   snapshot đêm qua nên cũ tối đa 24h. Lệch này bằng 0 đúng ở vùng feature nhắm tới
   (thẻ ngủ 60 ngày rồi thức dậy: cả hai bên đều trả ~60 ngày) và chỉ khác ở thẻ
   hoạt động hằng ngày — nơi feature không mang tính quyết định.

Cửa sổ 5 phút thì KHÔNG có khe hở: sorted set lúc serve tính đúng ``[t-300, t]``
gồm cả giao dịch hiện tại, y hệt ``rangeBetween(-300, 0)`` ở đây. Có test canh:
``tests/test_velocity_parity.py``.

Chạy trong container spark-master::

    spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.4 \
      --conf spark.jars.ivy=/tmp/.ivy2 \
      /opt/spark/jobs/dp3_training_features.py --lookback-days 400
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

# Độ dài cửa sổ khai ở MỘT chỗ (xem data_pipelines/shared/feature_windows.py).
sys.path.insert(0, os.environ.get("SHARED_DIR", "/opt/spark/shared"))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))
import feature_windows as W  # noqa: E402

GOLD = "s3a://curated"
FACT = f"{GOLD}/fact_transactions"
PG_SCHEMA = "application"
TABLE = "feat_training"


def build_spark() -> SparkSession:
    """SparkSession cấu hình s3a (đọc Gold); JDBC Postgres khi ghi."""
    ak = os.environ.get("AWS_ACCESS_KEY_ID") or os.environ["MINIO_ROOT_USER"]
    sk = os.environ.get("AWS_SECRET_ACCESS_KEY") or os.environ["MINIO_ROOT_PASSWORD"]
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    return (
        SparkSession.builder.appName("dp3_training_features")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", ak)
        .config("spark.hadoop.fs.s3a.secret.key", sk)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.sql.shuffle.partitions", "48")
        .getOrCreate()
    )


def jdbc_props() -> tuple[str, dict]:
    """(url, properties) cho JDBC tới Postgres warehouse."""
    host = os.environ.get("PG_HOST", "postgres")
    db = os.environ.get("PG_DB", "warehouse")
    return f"jdbc:postgresql://{host}:5432/{db}", {
        "user": os.environ.get("PG_USER") or os.environ["AIRFLOW_USER"],
        "password": os.environ.get("PG_PASSWORD") or os.environ["AIRFLOW_PASSWORD"],
        "driver": "org.postgresql.Driver",
    }


def current_dim(spark: SparkSession, name: str):
    """Bản CURRENT của một SCD2 dim.

    Dùng bản current cho thuộc tính TĨNH (brand, category, segment...) là chấp nhận
    được: chúng gần như không đổi, và nếu có đổi thì lệch nhỏ hơn nhiều so với chi
    phí một as-of join theo ``valid_from_ts``/``valid_to_ts``.
    """
    return spark.read.parquet(f"{GOLD}/{name}").filter("is_current = true")


def _rolling(part: list[str], seconds: int) -> Window:
    """Cửa sổ trượt theo EVENT-TIME ``[ts - seconds, ts]`` — GỒM dòng hiện tại.

    Dùng ``rangeBetween`` trên cột giây (không phải ``rowsBetween``) mới đúng nghĩa
    "N giây gần nhất": số dòng nằm trong cửa sổ thay đổi theo mật độ giao dịch.

    Dùng cho tầng batch và tầng **5 phút**: lúc serve, API ``ZADD`` giao dịch hiện
    tại vào sorted set TRƯỚC khi đọc, nên serving có tính nó.
    """
    return Window.partitionBy(*part).orderBy("ts").rangeBetween(-seconds, 0)


def _rolling_excl_self(part: list[str], seconds: int) -> Window:
    """Cửa sổ trượt ``[ts - seconds, ts - 1]`` — KHÔNG gồm dòng hiện tại.

    Dùng cho tầng **Flink** (merchant 10 phút, device 1 giờ). Lúc serve, giá trị
    trong Redis là của window đã CHỐT trước khi giao dịch hiện tại tới, nên Flink
    không thể đã tính nó.

    Vì sao chi tiết này quan trọng: ở nhịp ~817 giao dịch/ngày trên 1500 merchant,
    hầu hết giao dịch xảy ra ở merchant đang im lặng. Nếu training dùng
    ``(-600, 0)`` thì train=1 trong khi serve=0 — lệch hệ thống trên gần như MỌI
    dòng, không chỉ lúc có tấn công. Xem SELF_INCLUSIVE_TIERS trong
    ``shared/feature_windows.py``.
    """
    return Window.partitionBy(*part).orderBy("ts").rangeBetween(-seconds, -1)


def main() -> None:
    parser = argparse.ArgumentParser(description="DP3b PIT features -> feat_training")
    parser.add_argument("--lookback-days", type=int, default=400,
                        help="Chỉ tính cho giao dịch trong N ngày gần nhất.")
    args = parser.parse_args()

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    fact = spark.read.option("mergeSchema", "true").parquet(FACT)
    max_date = fact.agg(F.max("event_date")).collect()[0][0]
    lower = (F.to_timestamp(F.lit(max_date)) + F.expr("INTERVAL 1 DAY")
             - F.expr(f"INTERVAL {args.lookback_days} DAY"))
    # ts là epoch giây kiểu DOUBLE (giữ microsecond), không phải long.
    #
    # Vì sao: ``rangeBetween`` so theo GIÁ TRỊ của cột order. Nếu làm tròn về giây
    # thì nhiều giao dịch cùng giây có cùng giá trị -> tất cả nằm trong cửa sổ của
    # nhau, kể cả giao dịch xảy ra SAU. Đó là rò rỉ tương lai vào quá khứ, và nó
    # làm training lệch khỏi serving (sorted set chỉ thấy giao dịch đã tới).
    # Giữ microsecond thì mỗi giao dịch có một mốc riêng và rò rỉ biến mất.
    # Xem tests/test_velocity_parity.py::test_exact_ties_are_known_divergence.
    tx = (fact.filter(F.col("created_at") >= lower)
              .withColumn("ts", F.col("created_at").cast("double")))
    n_tx = tx.count()
    print(f"[DP3b] {n_tx:,} giao dịch (tới {max_date})")

    # --- cửa sổ -------------------------------------------------------------- #
    card_90 = _rolling(["card_id"], W.CARD_LONG_WINDOW_S)
    card_7 = _rolling(["card_id"], W.CARD_SHORT_WINDOW_S)
    card_5m = _rolling(["card_id"], W.CARD_VELOCITY_WINDOW_S)
    user_30 = _rolling(["user_id"], W.USER_WINDOW_S)
    merch_30 = _rolling(["merchant_id"], W.MERCHANT_WINDOW_S)
    dev_30 = _rolling(["device_id"], W.DEVICE_WINDOW_S)
    # tầng Flink -> KHÔNG gồm giao dịch hiện tại (xem _rolling_excl_self)
    merch_10m = _rolling_excl_self(["merchant_id"], W.MERCHANT_RT_WINDOW_S)
    dev_1h = _rolling_excl_self(["device_id"], W.DEVICE_RT_WINDOW_S)
    # "lần trước": mọi dòng TRƯỚC dòng hiện tại (không gồm nó)
    card_prev = Window.partitionBy("card_id").orderBy("ts").rowsBetween(
        Window.unboundedPreceding, -1)
    user_prev = Window.partitionBy("user_id").orderBy("ts").rowsBetween(
        Window.unboundedPreceding, -1)
    # "lần đầu xuất hiện": mọi dòng tính cả dòng hiện tại
    dev_all = Window.partitionBy("device_id").orderBy("ts").rowsBetween(
        Window.unboundedPreceding, Window.currentRow)

    def n_distinct(col: str, win: Window):
        """countDistinct KHÔNG dùng được trong window function -> size(collect_set)."""
        return F.size(F.collect_set(col).over(win)).cast("long")

    out = (
        tx
        # ---- card: baseline 90 ngày ----
        .withColumn("card_tx_count_90d", F.count(F.lit(1)).over(card_90).cast("long"))
        .withColumn("card_amount_sum_90d", F.sum("amount_usd").over(card_90))
        .withColumn("card_amount_avg_90d", F.avg("amount_usd").over(card_90))
        .withColumn("card_amount_max_90d", F.max("amount_usd").over(card_90))
        .withColumn("card_amount_std_90d",
                    F.coalesce(F.stddev("amount_usd").over(card_90), F.lit(0.0)))
        .withColumn("card_distinct_merchant_90d", n_distinct("merchant_id", card_90))
        # ---- card: nhịp 7 ngày (ghép với 90d -> tăng tốc, bắt bust-out) ----
        .withColumn("card_tx_count_7d", F.count(F.lit(1)).over(card_7).cast("long"))
        # ---- card: giao dịch NGAY TRƯỚC (dormant -> active) ----
        .withColumn("card_last_tx_at", F.max("created_at").over(card_prev))
        # ---- card: velocity 5 phút (serve = Redis sorted set) ----
        .withColumn("card_tx_count_5min", F.count(F.lit(1)).over(card_5m).cast("long"))
        .withColumn("card_amount_sum_5min", F.sum("amount_usd").over(card_5m))
        .withColumn("card_amount_avg_5min", F.avg("amount_usd").over(card_5m))
        # ---- user: baseline + graph 30 ngày ----
        .withColumn("user_tx_count_30d", F.count(F.lit(1)).over(user_30).cast("long"))
        .withColumn("user_amount_avg_30d", F.avg("amount_usd").over(user_30))
        .withColumn("user_device_count_30d", n_distinct("device_id", user_30))
        .withColumn("user_distinct_country_30d", n_distinct("ip_country_code", user_30))
        .withColumn("user_last_tx_at", F.max("created_at").over(user_prev))
        # ---- merchant: baseline 30 ngày ----
        .withColumn("merchant_tx_count_30d", F.count(F.lit(1)).over(merch_30).cast("long"))
        .withColumn("merchant_amount_avg_30d", F.avg("amount_usd").over(merch_30))
        .withColumn("merchant_amount_std_30d",
                    F.coalesce(F.stddev("amount_usd").over(merch_30), F.lit(0.0)))
        .withColumn("merchant_distinct_cards_30d", n_distinct("card_id", merch_30))
        # ---- merchant: real-time 10 phút (serve = Flink) ----
        .withColumn("merch_tx_count_10min", F.count(F.lit(1)).over(merch_10m).cast("long"))
        .withColumn("merch_distinct_cards_10min", n_distinct("card_id", merch_10m))
        # cửa sổ rỗng (merchant im lặng) -> avg là null; serving trả 0 khi giá trị
        # Flink quá hạn, nên training phải là 0 chứ không phải null
        .withColumn("merch_amount_avg_10min",
                    F.coalesce(F.avg("amount_usd").over(merch_10m), F.lit(0.0)))
        # ---- device: graph 30 ngày ----
        .withColumn("device_tx_count_30d", F.count(F.lit(1)).over(dev_30).cast("long"))
        .withColumn("device_distinct_users_30d", n_distinct("user_id", dev_30))
        .withColumn("device_distinct_cards_30d", n_distinct("card_id", dev_30))
        .withColumn("device_first_seen_at", F.min("created_at").over(dev_all))
        # ---- device: real-time 1 giờ (serve = Flink) ----
        .withColumn("device_tx_count_1h", F.count(F.lit(1)).over(dev_1h).cast("long"))
        .withColumn("device_distinct_users_1h", n_distinct("user_id", dev_1h))
        .withColumn("device_distinct_cards_1h", n_distinct("card_id", dev_1h))
    )

    # --- thuộc tính dim (tĩnh theo thời gian -> join thẳng) ------------------- #
    dim_card = current_dim(spark, "dim_card").select(
        F.col("id").alias("card_id"), F.col("brand").alias("card_brand"),
        F.col("type").alias("card_type"), "is_virtual",
        F.col("created_at").alias("card_created_at"))
    dim_user = current_dim(spark, "dim_user").select(
        F.col("id").alias("user_id"), "customer_segment", "kyc_level", "email_verified",
        F.col("country_code").alias("user_country"),
        F.col("created_at").alias("account_created_at"))
    dim_merchant = current_dim(spark, "dim_merchant").select(
        F.col("id").alias("merchant_id"), F.col("category").alias("merchant_category"),
        F.col("risk_level").alias("merchant_risk_level"))

    out = (out.join(F.broadcast(dim_card), "card_id", "left")
              .join(F.broadcast(dim_user), "user_id", "left")
              .join(F.broadcast(dim_merchant), "merchant_id", "left"))

    out = out.select(
        F.col("id").alias("transaction_id"),
        "user_id", "card_id", "merchant_id", "device_id",
        F.col("created_at").alias("event_timestamp"),
        # --- cột thô của giao dịch (đầu vào cho feature on-demand) ---
        "amount_usd", "channel", "billing_country_code", "ip_country_code",
        "email_purchaser", "email_recipient", "auth_3ds_flag",
        # --- dim tĩnh ---
        "card_brand", "card_type", "is_virtual", "card_created_at",
        "customer_segment", "kyc_level", "email_verified", "user_country",
        "account_created_at", "merchant_category", "merchant_risk_level",
        # --- batch: card ---
        "card_tx_count_90d", "card_amount_sum_90d", "card_amount_avg_90d",
        "card_amount_max_90d", "card_amount_std_90d", "card_distinct_merchant_90d",
        "card_tx_count_7d", "card_last_tx_at",
        # --- batch: user ---
        "user_tx_count_30d", "user_amount_avg_30d", "user_device_count_30d",
        "user_distinct_country_30d", "user_last_tx_at",
        # --- batch: merchant ---
        "merchant_tx_count_30d", "merchant_amount_avg_30d", "merchant_amount_std_30d",
        "merchant_distinct_cards_30d",
        # --- batch: device ---
        "device_tx_count_30d", "device_distinct_users_30d", "device_distinct_cards_30d",
        "device_first_seen_at",
        # --- real-time: Flink lúc serve ---
        "merch_tx_count_10min", "merch_distinct_cards_10min", "merch_amount_avg_10min",
        "device_tx_count_1h", "device_distinct_users_1h", "device_distinct_cards_1h",
        # --- đồng bộ: sorted set lúc serve ---
        "card_tx_count_5min", "card_amount_sum_5min", "card_amount_avg_5min",
        F.current_timestamp().alias("created"))

    url, props = jdbc_props()
    out.write.mode("overwrite").option("truncate", "false").jdbc(
        url, f"{PG_SCHEMA}.{TABLE}", properties=props)
    print(f"[DP3b] {PG_SCHEMA}.{TABLE}: {n_tx:,} dòng, {len(out.columns)} cột")

    spark.stop()


if __name__ == "__main__":
    main()
