"""DP2 (Bronze -> Silver) cho transactions: **merge schema + dedup**.

Đọc ``raw/transactions`` (Bronze) từ GCS, hợp nhất schema giữa các partition
(partition cũ thiếu ``auth_3ds_flag`` -> điền null), khử trùng lặp theo ``id``
(bỏ ~1% duplicate đã tiêm), rồi ghi ``staging/transactions`` (Silver) partition
theo ``event_date``.

Chạy như batch Dataproc Serverless (DAG ml_pipeline tự submit)::

    gcloud dataproc batches submit pyspark \\
      $DATAPROC_CODE_ROOT/dp2_bronze_to_silver.py \\
      --region=$DATAPROC_REGION \\
      --py-files=$DATAPROC_PYFILES --jars=$DATAPROC_JDBC_JAR \\
      -- --date all        # backfill toàn bộ
    gcloud dataproc batches submit pyspark \\
      $DATAPROC_CODE_ROOT/dp2_bronze_to_silver.py \\
      --region=$DATAPROC_REGION \\
      --py-files=$DATAPROC_PYFILES --jars=$DATAPROC_JDBC_JAR \\
      -- --date 2026-07-23  # incremental 1 ngày

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

    Không set cấu hình filesystem nào: Dataproc Serverless đã có sẵn
    gcs-connector và tự dùng service account của job.
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
