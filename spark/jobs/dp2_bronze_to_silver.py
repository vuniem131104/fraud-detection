"""DP2 (Bronze -> Silver) cho transactions: **merge schema + dedup**.

Đọc ``raw/transactions`` (Bronze) từ GCS, hợp nhất schema giữa các partition
(partition cũ thiếu ``auth_3ds_flag`` -> điền null), khử trùng lặp theo ``id``
(bỏ ~1% duplicate đã tiêm), rồi ghi ``staging/transactions`` (Silver) partition
theo ``event_date``.

Chạy bằng spark-submit trong container Airflow (DAG ml_pipeline tự gọi, xem
`spark_task`)::

    spark-submit ... dp2_bronze_to_silver.py --date all         # backfill
    spark-submit ... dp2_bronze_to_silver.py --date 2026-08-08  # 1 ngày

Đường dẫn data lake lấy từ env LAKE_ROOT (gs://...).
"""

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType

# Đường dẫn data lake trên GCS. LAKE_ROOT quyết định layout (1 bucket 4 prefix
# hoặc 4 bucket riêng) — xem airflow/include/lake.py.
LAKE_ROOT = os.environ["LAKE_ROOT"]
if not LAKE_ROOT.endswith("/"):
    LAKE_ROOT += "/"

RAW = f"{LAKE_ROOT}raw/transactions"
STG = f"{LAKE_ROOT}staging/transactions"


def build_spark() -> SparkSession:
    """SparkSession đọc/ghi GCS.

    Cấu hình filesystem (gcs-connector + auth ADC) do spark-submit truyền
    vào bằng --conf, xem SPARK_CONF trong airflow/dags/ml_pipeline.py.
    """
    return (
        SparkSession.builder.appName("dp2_bronze_to_silver")
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
    #
    # Hai con số này chỉ để log, nhưng viết thành `df.count()` rồi
    # `df.dropDuplicates(...).count()` thì mỗi cái là MỘT action riêng: đo trên
    # history server (app dp2_bronze_to_silver, backfill 100k dòng) thấy các stage
    # `count` ngốn 39,9s / 124,9s executor time = 32% cả job, chỉ để in một dòng.
    # Gộp vào một `agg` -> một action duy nhất quét một lượt.
    stats = df.agg(F.count(F.lit(1)).alias("n"),
                   F.countDistinct("id").alias("uniq")).first()
    before, after = stats["n"], stats["uniq"]
    df = df.dropDuplicates(["id"])
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
