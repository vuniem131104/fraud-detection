"""DP2 (Bronze -> Silver) cho transactions: **merge schema + dedup**.

Đọc ``raw/transactions`` (Bronze) từ MinIO, hợp nhất schema giữa các partition
(partition cũ thiếu ``auth_3ds_flag`` -> điền null), khử trùng lặp theo ``id``
(bỏ ~1% duplicate đã tiêm), rồi ghi ``staging/transactions`` (Silver) partition
theo ``event_date``.

Chạy trong container spark-master::

    spark-submit --master spark://spark-master:7077 \
      --packages org.apache.hadoop:hadoop-aws:3.3.4 \
      --conf spark.jars.ivy=/tmp/.ivy2 \
      /opt/spark/jobs/dp2_bronze_to_silver.py --date all        # backfill toàn bộ
      /opt/spark/jobs/dp2_bronze_to_silver.py --date 2026-07-23  # incremental 1 ngày

Cred MinIO đọc từ env AWS_ACCESS_KEY_ID/SECRET (hoặc MINIO_ROOT_USER/PASSWORD).
"""

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType

RAW = "s3a://raw/transactions"
STG = "s3a://staging/transactions"


def build_spark() -> SparkSession:
    """SparkSession cấu hình s3a trỏ vào MinIO."""
    ak = os.environ.get("AWS_ACCESS_KEY_ID") or os.environ["MINIO_ROOT_USER"]
    sk = os.environ.get("AWS_SECRET_ACCESS_KEY") or os.environ["MINIO_ROOT_PASSWORD"]
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    return (
        SparkSession.builder.appName("dp2_bronze_to_silver")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", ak)
        .config("spark.hadoop.fs.s3a.secret.key", sk)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        # chỉ ghi đè partition có trong DF (an toàn cho incremental)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="DP2 Bronze->Silver: merge schema + dedup")
    parser.add_argument("--date", default="all",
                        help="'all' để backfill toàn bộ, hoặc YYYY-MM-DD cho 1 ngày.")
    args = parser.parse_args()

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # --- đọc Bronze ---
    if args.date == "all":
        # mergeSchema: hợp nhất schema mọi partition (cũ thiếu auth_3ds_flag -> null)
        df = spark.read.option("mergeSchema", "true").parquet(RAW)
    else:
        # 1 partition: event_date nằm ở path -> tự thêm lại thành cột
        df = (spark.read.parquet(f"{RAW}/event_date={args.date}")
              .withColumn("event_date", F.lit(args.date)))

    # --- merge schema: đảm bảo auth_3ds_flag luôn tồn tại (null cho data cũ) ---
    if "auth_3ds_flag" not in df.columns:
        df = df.withColumn("auth_3ds_flag", F.lit(None).cast(BooleanType()))

    # --- dedup theo id (bỏ duplicate đã tiêm) ---
    before = df.count()
    df = df.dropDuplicates(["id"])
    after = df.count()
    print(f"[DP2] date={args.date}: rows {before:,} -> {after:,} "
          f"(removed {before - after:,} duplicates)")

    # 1 file/partition cho gọn (tránh nổ small files từ shuffle của dropDuplicates)
    (df.repartition("event_date")
       .write.mode("overwrite").partitionBy("event_date")
       .option("mergeSchema", "true").parquet(STG))
    print(f"[DP2] wrote Silver -> {STG}")

    spark.stop()


if __name__ == "__main__":
    main()
