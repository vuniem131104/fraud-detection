"""DP3a — Gold -> bảng feature SERVING (``application.feat_*``) trong Postgres.

Bốn bảng, mỗi entity một bảng, **một dòng cho mỗi entity** = trạng thái *as-of
hôm nay*. Đây là thứ **serving** cần: lúc chấm điểm ta chỉ hỏi "thẻ này bây giờ
thế nào", không hỏi lịch sử. Bốn bảng này được ``feast materialize`` đẩy lên Redis.

    feat_card     (card_id)     dim thẻ + aggregate 7d/90d
    feat_user     (user_id)     dim khách + aggregate/graph 30d
    feat_merchant (merchant_id) dim merchant + aggregate 30d
    feat_device   (device_id)   graph 30d + lần đầu xuất hiện

Nguyên tắc thiết kế: bảng batch chỉ chứa **baseline**, không chứa feature "thông
minh". `amount_usd = 500` chẳng nói gì; `500` trên một thẻ có
``card_amount_avg_90d = 12`` thì nói rất nhiều. Tỉ lệ được tính trong ODFV lúc
request (xem ``feature_store/feature_views.py``) nên công thức chỉ tồn tại một chỗ.

Base trên **dim current** chứ không phải trên fact, để MỌI entity đều có dòng:
thẻ không hoạt động 90 ngày phải trả về 0, không phải "tra không thấy". Serving
phân biệt được hai thứ đó thì logic mới rõ ràng.

KHÔNG lưu sẵn: ``account_age_days`` / ``card_age_days`` / ``device_age_hours``
(đổi mỗi giây) -> lưu ``*_created_at`` / ``*_first_seen_at`` rồi trừ lúc request.

Chạy trong container spark-master::

    spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.4 \
      --conf spark.jars.ivy=/tmp/.ivy2 \
      /opt/spark/jobs/dp3_gold_to_features.py --date all
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Độ dài cửa sổ khai ở MỘT chỗ (xem data_pipelines/shared/feature_windows.py).
sys.path.insert(0, os.environ.get("SHARED_DIR", "/opt/spark/shared"))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))
import feature_windows as W  # noqa: E402

# Đường dẫn data lake: MinIO ở local (s3a://), GCS trên GCP (gs://<bucket>/).
# Một biến LAKE_ROOT quyết định cả scheme lẫn cách tổ chức bucket — xem
# data_pipelines/airflow/include/lake.py. Job KHÔNG hardcode scheme nào để chạy
# được cả trên cụm Spark local lẫn Dataproc Serverless.
LAKE_ROOT = os.environ.get("LAKE_ROOT") or "s3a://"
if not LAKE_ROOT.endswith("/"):
    LAKE_ROOT += "/"

GOLD = f"{LAKE_ROOT}curated"
FACT = f"{GOLD}/fact_transactions"
PG_SCHEMA = "application"


def _base_builder():
    """Builder với mọi cấu hình KHÔNG phụ thuộc backend storage."""
    b = SparkSession.builder.appName("dp3_gold_to_features")
    b = b.config("spark.sql.shuffle.partitions", "32")
    return b


def build_spark() -> SparkSession:
    """SparkSession cấu hình s3a (đọc Gold); JDBC Postgres cấu hình khi ghi."""
    # GCS: Dataproc đã có gcs-connector và tự dùng service account của job, nên
    # KHÔNG set gì thêm — và tuyệt đối không đọc MINIO_* (không tồn tại trên GCP,
    # os.environ[...] sẽ ném KeyError trước khi job kịp chạy).
    if LAKE_ROOT.startswith("gs://"):
        return _base_builder().getOrCreate()

    ak = os.environ.get("AWS_ACCESS_KEY_ID") or os.environ["MINIO_ROOT_USER"]
    sk = os.environ.get("AWS_SECRET_ACCESS_KEY") or os.environ["MINIO_ROOT_PASSWORD"]
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    return (
        _base_builder()
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", ak)
        .config("spark.hadoop.fs.s3a.secret.key", sk)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
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


def write_pg(df, table: str) -> None:
    """Ghi df vào ``application.<table>`` (overwrite — bảng serving là snapshot)."""
    url, props = jdbc_props()
    n = df.count()
    (df.write.mode("overwrite").option("truncate", "false")
       .jdbc(url, f"{PG_SCHEMA}.{table}", properties=props))
    print(f"[DP3a] {PG_SCHEMA}.{table}: {n:,} dòng")


def current_dim(spark: SparkSession, name: str):
    """Bản CURRENT (is_current=true) của một SCD2 dim ở Gold."""
    return spark.read.parquet(f"{GOLD}/{name}").filter("is_current = true")


def main() -> None:
    parser = argparse.ArgumentParser(description="DP3a Gold -> feat_* (serving snapshot)")
    parser.add_argument("--date", default="all",
                        help="'all' (as-of ngày mới nhất trong fact) hoặc YYYY-MM-DD.")
    args = parser.parse_args()

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    fact = spark.read.option("mergeSchema", "true").parquet(FACT)
    ref = args.date if args.date != "all" else fact.agg(F.max("event_date")).collect()[0][0]

    # upper là 00:00 của NGÀY HÔM SAU -> cửa sổ phủ trọn ngày ref
    upper = F.to_timestamp(F.lit(ref)) + F.expr("INTERVAL 1 DAY")
    ev = F.to_timestamp(F.lit(ref))          # event_timestamp của snapshot
    now = F.current_timestamp()
    print(f"[DP3a] as-of ref={ref}")

    def window(days: int):
        return fact.filter((F.col("created_at") >= upper - F.expr(f"INTERVAL {days} DAY"))
                           & (F.col("created_at") < upper))

    w7 = window(W.CARD_SHORT_WINDOW_D)
    w30 = window(W.USER_WINDOW_D)            # = MERCHANT/DEVICE_WINDOW_D (30)
    w90 = window(W.CARD_LONG_WINDOW_D)

    def stamp(df):
        return df.withColumn("event_timestamp", ev).withColumn("created", now)

    # ---------------------------------------------------------------- feat_card
    agg90 = w90.groupBy("card_id").agg(
        F.count("*").cast("long").alias("card_tx_count_90d"),
        F.sum("amount_usd").alias("card_amount_sum_90d"),
        F.avg("amount_usd").alias("card_amount_avg_90d"),
        F.max("amount_usd").alias("card_amount_max_90d"),
        # stddev_samp: thẻ có đúng 1 giao dịch -> null -> fillna 0 bên dưới
        F.stddev("amount_usd").alias("card_amount_std_90d"),
        F.countDistinct("merchant_id").cast("long").alias("card_distinct_merchant_90d"),
        F.max("created_at").alias("card_last_tx_at"))
    # cửa sổ 7 ngày: ghép với 90d thành "tỉ lệ tăng tốc" -> bắt bust-out
    agg7 = w7.groupBy("card_id").agg(
        F.count("*").cast("long").alias("card_tx_count_7d"))
    dim_card = current_dim(spark, "dim_card").select(
        F.col("id").alias("card_id"), F.col("brand").alias("card_brand"),
        F.col("type").alias("card_type"), "is_virtual",
        F.col("created_at").alias("card_created_at"))
    feat_card = stamp(dim_card
                      .join(agg90, "card_id", "left").join(agg7, "card_id", "left")
                      .fillna({"card_tx_count_90d": 0, "card_tx_count_7d": 0,
                               "card_amount_sum_90d": 0.0, "card_amount_avg_90d": 0.0,
                               "card_amount_max_90d": 0.0, "card_amount_std_90d": 0.0,
                               "card_distinct_merchant_90d": 0}))
    write_pg(feat_card, "feat_card")

    # ---------------------------------------------------------------- feat_user
    agg_user = w30.groupBy("user_id").agg(
        F.count("*").cast("long").alias("user_tx_count_30d"),
        F.avg("amount_usd").alias("user_amount_avg_30d"),
        F.countDistinct("device_id").cast("long").alias("user_device_count_30d"),
        # "user này bình thường giao dịch từ mấy nước" -> mẫu số cho geo_mismatch:
        # người hay đi công tác 4 nước thì lệch quốc gia không đáng lo
        F.countDistinct("ip_country_code").cast("long").alias("user_distinct_country_30d"),
        F.max("created_at").alias("user_last_tx_at"))
    dim_user = current_dim(spark, "dim_user").select(
        F.col("id").alias("user_id"), "customer_segment", "kyc_level", "email_verified",
        F.col("country_code").alias("user_country"),
        F.col("created_at").alias("account_created_at"))
    feat_user = stamp(dim_user.join(agg_user, "user_id", "left")
                      .fillna({"user_tx_count_30d": 0, "user_amount_avg_30d": 0.0,
                               "user_device_count_30d": 0, "user_distinct_country_30d": 0}))
    write_pg(feat_user, "feat_user")

    # ------------------------------------------------------------ feat_merchant
    agg_merch = w30.groupBy("merchant_id").agg(
        F.count("*").cast("long").alias("merchant_tx_count_30d"),
        F.avg("amount_usd").alias("merchant_amount_avg_30d"),
        F.stddev("amount_usd").alias("merchant_amount_std_30d"),
        F.countDistinct("card_id").cast("long").alias("merchant_distinct_cards_30d"))
    dim_merch = current_dim(spark, "dim_merchant").select(
        F.col("id").alias("merchant_id"), F.col("category").alias("merchant_category"),
        F.col("risk_level").alias("merchant_risk_level"))
    feat_merchant = stamp(dim_merch.join(agg_merch, "merchant_id", "left")
                          .fillna({"merchant_tx_count_30d": 0,
                                   "merchant_amount_avg_30d": 0.0,
                                   "merchant_amount_std_30d": 0.0,
                                   "merchant_distinct_cards_30d": 0}))
    write_pg(feat_merchant, "feat_merchant")

    # -------------------------------------------------------------- feat_device
    agg_dev = w30.groupBy("device_id").agg(
        F.count("*").cast("long").alias("device_tx_count_30d"),
        F.countDistinct("user_id").cast("long").alias("device_distinct_users_30d"),
        # distinct CARDS mạnh hơn distinct users cho fraud ring: device farm quay
        # vòng 40 thẻ trộm nhưng có thể chỉ dựng 3-4 "user"
        F.countDistinct("card_id").cast("long").alias("device_distinct_cards_30d"))
    # Lần đầu xuất hiện tính trên TOÀN lịch sử, không phải 30 ngày: device thấy
    # lần đầu 6 tháng trước không được trông như device mới.
    first_seen = fact.groupBy("device_id").agg(
        F.min("created_at").alias("device_first_seen_at"))
    dim_dev = current_dim(spark, "dim_device").select(F.col("id").alias("device_id"))
    feat_device = stamp(dim_dev
                        .join(agg_dev, "device_id", "left")
                        .join(first_seen, "device_id", "left")
                        .fillna({"device_tx_count_30d": 0, "device_distinct_users_30d": 0,
                                 "device_distinct_cards_30d": 0}))
    write_pg(feat_device, "feat_device")

    spark.stop()


if __name__ == "__main__":
    main()
