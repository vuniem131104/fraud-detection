"""DEMO — đổ star schema từ Gold (GCS) xuống Postgres để truy vấn/BI.

Chạy MỘT LẦN bằng tay, không nằm trong DAG nào. Mục đích là có một star schema
truy vấn được bằng SQL thường (join dim với fact), thứ mà lake trên GCS không cho.

Ghi vào đâu và VÌ SAO
---------------------
``warehouse`` / schema **``dwh``** — không phải ``application``.

``application`` là contract của FEATURE SERVING: Feast đọc ``feat_*`` từ đó, DP3
``overwrite`` thẳng lên các bảng đó, và task ``validate`` của DAG khẳng định số
dòng ở đó khác 0. Nhét thêm ``dim_*``/``fact_*`` vào cùng schema thì một lệnh
``feast materialize`` hay một lần DP3 chạy nhầm sẽ đụng vào bảng BI, và tên bảng
trong cùng một namespace không còn nói được cái nào thuộc hệ nào.

Không tạo database thứ 5 vì như vậy phải thêm biến ``.env``, thêm nhánh bootstrap
DDL, thêm một check trong ``check_connections.py``. Một schema thì miễn phí.

Nguồn là tầng **curated (Gold)**, không phải staging: dim ở Gold đã là SCD2
(``valid_from_ts`` / ``valid_to_ts`` / ``is_current``) và fact đã dedup + partition
theo ``event_date``. Đó chính là mô hình chiều cần đổ xuống.

Chạy (helper ``sparkjob`` xem README §7.0)::

    sparkjob dwh_gold_to_postgres.py
    sparkjob dwh_gold_to_postgres.py --schema dwh --only fact_transactions

Mỗi bảng ghi bằng ``mode("overwrite")`` nên chạy lại là idempotent: Spark DROP rồi
CREATE lại. Đổi lại, bảng do Spark tạo KHÔNG có primary key hay index — ở quy mô
này (100k dòng fact) seq scan vẫn tính bằng mili giây. Muốn có ràng buộc thật thì
tạo bảng trước bằng DDL rồi đổi sang ``.option("truncate", "true")``.
"""

from __future__ import annotations

import argparse
import os

import psycopg
from pyspark.sql import SparkSession

LAKE_ROOT = os.environ["LAKE_ROOT"]
if not LAKE_ROOT.endswith("/"):
    LAKE_ROOT += "/"

GOLD = f"{LAKE_ROOT}curated"

# dataset ở Gold -> bảng trong Postgres. Giữ nguyên tên để truy ngược dễ.
TABLES = ["dim_user", "dim_card", "dim_merchant", "dim_device", "fact_transactions"]


def build_spark() -> SparkSession:
    """SparkSession đọc GCS + ghi JDBC (cấu hình filesystem do spark-submit truyền)."""
    return SparkSession.builder.appName("dwh_gold_to_postgres").getOrCreate()


def jdbc_props() -> tuple[str, dict]:
    """(url, properties) cho JDBC tới Postgres warehouse — giống hệt dp3_*."""
    host = os.environ.get("PG_HOST", "postgres")
    db = os.environ.get("PG_DB", "warehouse")
    return f"jdbc:postgresql://{host}:5432/{db}", {
        "user": os.environ.get("PG_USER") or os.environ["AIRFLOW_USER"],
        "password": os.environ.get("PG_PASSWORD") or os.environ["AIRFLOW_PASSWORD"],
        "driver": "org.postgresql.Driver",
    }


def ensure_schema(schema: str) -> None:
    """Tạo schema nếu chưa có.

    Writer JDBC của Spark tạo được BẢNG nhưng không tạo SCHEMA — thiếu bước này
    thì lỗi là ``schema "dwh" does not exist`` ngay ở bảng đầu tiên.
    """
    host = os.environ.get("PG_HOST", "postgres")
    db = os.environ.get("PG_DB", "warehouse")
    user = os.environ.get("PG_USER") or os.environ["AIRFLOW_USER"]
    pwd = os.environ.get("PG_PASSWORD") or os.environ["AIRFLOW_PASSWORD"]
    with psycopg.connect(f"host={host} port=5432 dbname={db} user={user} password={pwd}",
                         autocommit=True) as conn:
        conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')


def row_counts(schema: str, tables: list[str]) -> dict[str, int]:
    """Đếm dòng SAU khi ghi, hỏi thẳng Postgres.

    Không dùng ``df.count()``: đó là một action Spark nữa, quét lại parquet trên
    GCS chỉ để in một con số. Hỏi đích đến vừa rẻ hơn vừa chứng minh dữ liệu đã
    thực sự nằm trong bảng.
    """
    host = os.environ.get("PG_HOST", "postgres")
    db = os.environ.get("PG_DB", "warehouse")
    user = os.environ.get("PG_USER") or os.environ["AIRFLOW_USER"]
    pwd = os.environ.get("PG_PASSWORD") or os.environ["AIRFLOW_PASSWORD"]
    out = {}
    with psycopg.connect(f"host={host} port=5432 dbname={db} user={user} password={pwd}") as conn:
        for t in tables:
            out[t] = conn.execute(f'SELECT count(*) FROM "{schema}"."{t}"').fetchone()[0]
    return out


def main() -> None:
    p = argparse.ArgumentParser(description="DEMO: Gold -> Postgres star schema")
    p.add_argument("--schema", default="dwh", help="schema đích trong database warehouse")
    p.add_argument("--only", help="chỉ ghi các bảng này (phân tách bằng dấu phẩy)")
    args = p.parse_args()

    tables = args.only.split(",") if args.only else TABLES
    unknown = [t for t in tables if t not in TABLES]
    if unknown:
        raise SystemExit(f"không biết bảng {unknown}; hợp lệ: {TABLES}")

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    ensure_schema(args.schema)
    url, props = jdbc_props()

    for t in tables:
        spark.sparkContext.setJobDescription(f"{t}: curated -> {args.schema}.{t}")
        df = spark.read.option("mergeSchema", "true").parquet(f"{GOLD}/{t}")
        # _row_hash là dấu vết nội bộ của SCD2 (dùng để phát hiện thay đổi), không
        # phải cột nghiệp vụ -> không đưa xuống bảng trình bày.
        df = df.drop(*[c for c in df.columns if c.startswith("_")])
        df.write.mode("overwrite").option("truncate", "false").jdbc(
            url, f"{args.schema}.{t}", properties=props)
        print(f"[DWH] {args.schema}.{t}: {len(df.columns)} cột")

    print(f"\n=== {args.schema} ===")
    for t, n in row_counts(args.schema, tables).items():
        print(f"  {t:<20} {n:>9,} dòng")

    spark.stop()


if __name__ == "__main__":
    main()
