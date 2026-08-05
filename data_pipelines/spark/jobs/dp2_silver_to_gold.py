"""DP2 (Silver -> Gold): **fact_transactions** + **SCD2 dim tables**.

  stage=fact : staging/transactions (Silver) -> curated/fact_transactions (Gold),
               partition theo event_date.
  stage=dims : raw/<dim>/snapshot (Bronze) -> curated/dim_* (Gold) theo SCD Type 2
               (thêm valid_from_ts, valid_to_ts, is_current). Lần đầu = initial load
               (mọi bản ghi is_current=true); lần sau nếu snapshot đổi -> đóng bản cũ,
               chèn bản mới.

Chạy trong container spark-master::

    spark-submit ... dp2_silver_to_gold.py --stage fact --date all
    spark-submit ... dp2_silver_to_gold.py --stage fact --date 2026-07-23
    spark-submit ... dp2_silver_to_gold.py --stage dims
"""

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

# Đường dẫn data lake: MinIO ở local (s3a://), GCS trên GCP (gs://<bucket>/).
# Một biến LAKE_ROOT quyết định cả scheme lẫn cách tổ chức bucket — xem
# data_pipelines/airflow/include/lake.py. Job KHÔNG hardcode scheme nào để chạy
# được cả trên cụm Spark local lẫn Dataproc Serverless.
LAKE_ROOT = os.environ.get("LAKE_ROOT") or "s3a://"
if not LAKE_ROOT.endswith("/"):
    LAKE_ROOT += "/"

SILVER = f"{LAKE_ROOT}staging/transactions"
BRONZE = f"{LAKE_ROOT}raw"
GOLD = f"{LAKE_ROOT}curated"

# gold dim name -> (thư mục snapshot ở Bronze, natural key)
DIMS = {
    "dim_user": ("users", "id"),
    "dim_card": ("cards", "id"),
    "dim_merchant": ("merchants", "id"),
    "dim_device": ("devices", "id"),
}


def _base_builder():
    """Builder với mọi cấu hình KHÔNG phụ thuộc backend storage."""
    b = SparkSession.builder.appName("dp2_silver_to_gold")
    b = b.config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    return b


def build_spark() -> SparkSession:
    """SparkSession trỏ vào data lake — MinIO (s3a) hoặc GCS."""
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


def path_exists(spark: SparkSession, path: str) -> bool:
    """True nếu path tồn tại VÀ có file thật (bỏ qua marker thư mục rỗng)."""
    jvm = spark._jvm
    hpath = jvm.org.apache.hadoop.fs.Path(path)
    fs = hpath.getFileSystem(spark._jsc.hadoopConfiguration())
    return fs.exists(hpath) and len(fs.listStatus(hpath)) > 0


def _delete_path(spark: SparkSession, path: str) -> None:
    """Xoá đệ quy 1 path trên s3a."""
    jvm = spark._jvm
    hpath = jvm.org.apache.hadoop.fs.Path(path)
    fs = hpath.getFileSystem(spark._jsc.hadoopConfiguration())
    if fs.exists(hpath):
        fs.delete(hpath, True)


def write_swap(spark: SparkSession, df, gpath: str) -> None:
    """Ghi df -> gpath an toàn khi df ĐỌC TỪ gpath (đọc-ghi cùng path).

    Spark không cho đọc + overwrite cùng path (lazy -> xoá nguồn trước khi đọc).
    Nên ghi ra <gpath>__tmp trước, rồi copy sang gpath (path khác -> an toàn).
    """
    tmp = f"{gpath}__tmp"
    df.write.mode("overwrite").parquet(tmp)
    spark.read.parquet(tmp).write.mode("overwrite").parquet(gpath)
    _delete_path(spark, tmp)


def build_fact(spark: SparkSession, date: str) -> None:
    """Silver transactions -> Gold fact_transactions (partition event_date)."""
    if date == "all":
        df = spark.read.option("mergeSchema", "true").parquet(SILVER)
    else:
        df = (spark.read.parquet(f"{SILVER}/event_date={date}")
              .withColumn("event_date", F.lit(date)))
    (df.repartition("event_date")
       .write.mode("overwrite").partitionBy("event_date")
       .option("mergeSchema", "true").parquet(f"{GOLD}/fact_transactions"))
    print(f"[DP2-gold] fact_transactions written (date={date})")


def scd2_merge(spark: SparkSession, gold_name: str, snap_dir: str, key: str) -> None:
    """Áp SCD Type 2 cho 1 dim: Bronze snapshot -> curated/<gold_name>."""
    inc = spark.read.parquet(f"{BRONZE}/{snap_dir}/snapshot.parquet")
    attrs = [c for c in inc.columns if c != key]
    # hash của toàn bộ thuộc tính (trừ key) để phát hiện thay đổi
    row_hash = F.sha2(F.concat_ws(
        "||", *[F.coalesce(F.col(c).cast("string"), F.lit("\u2400")) for c in attrs]), 256)
    inc = inc.withColumn("_row_hash", row_hash)
    gpath = f"{GOLD}/{gold_name}"
    now = F.current_timestamp()

    # --- lần đầu: initial load, mọi bản ghi là current ---
    if not path_exists(spark, gpath):
        out = (inc.withColumn("valid_from_ts", now)
                  .withColumn("valid_to_ts", F.lit(None).cast(TimestampType()))
                  .withColumn("is_current", F.lit(True)))
        out.write.mode("overwrite").parquet(gpath)
        print(f"[DP2-gold] {gold_name}: initial load {out.count():,} rows")
        return

    # --- các lần sau: SCD2 merge ---
    cur = spark.read.parquet(gpath)
    cur_current = cur.filter("is_current = true")
    hist = cur.filter("is_current = false")           # bản đã đóng -> giữ nguyên

    cmp = (cur_current.select(key, F.col("_row_hash").alias("cur_hash"))
           .join(inc.select(key, F.col("_row_hash").alias("inc_hash")), key, "full_outer"))
    changed_keys = cmp.filter(
        F.col("cur_hash").isNotNull() & F.col("inc_hash").isNotNull()
        & (F.col("cur_hash") != F.col("inc_hash"))).select(key)
    new_keys = cmp.filter(F.col("cur_hash").isNull()).select(key)   # chỉ có ở incoming
    upsert_keys = changed_keys.unionByName(new_keys)

    # bản current bị đổi -> đóng lại (valid_to, is_current=false)
    closed = (cur_current.join(F.broadcast(changed_keys), key, "left_semi")
              .withColumn("valid_to_ts", now).withColumn("is_current", F.lit(False)))
    # bản current không đổi -> giữ nguyên
    kept = cur_current.join(F.broadcast(changed_keys), key, "left_anti")
    # bản mới (đổi hoặc key mới) -> chèn version current
    new_versions = (inc.join(F.broadcast(upsert_keys), key, "left_semi")
                    .withColumn("valid_from_ts", now)
                    .withColumn("valid_to_ts", F.lit(None).cast(TimestampType()))
                    .withColumn("is_current", F.lit(True)))

    n_changed, n_new = changed_keys.count(), new_keys.count()
    out = hist.unionByName(closed).unionByName(kept).unionByName(new_versions)
    write_swap(spark, out, gpath)          # đọc-ghi cùng path -> qua temp
    print(f"[DP2-gold] {gold_name}: SCD2 merge -> changed={n_changed} new={n_new}")


def main() -> None:
    parser = argparse.ArgumentParser(description="DP2 Silver->Gold: fact + SCD2 dims")
    parser.add_argument("--stage", choices=["fact", "dims"], required=True)
    parser.add_argument("--date", default="all", help="cho stage=fact: 'all' hoặc YYYY-MM-DD")
    args = parser.parse_args()

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    if args.stage == "fact":
        build_fact(spark, args.date)
    else:
        for gold_name, (snap_dir, key) in DIMS.items():
            scd2_merge(spark, gold_name, snap_dir, key)

    spark.stop()


if __name__ == "__main__":
    main()
