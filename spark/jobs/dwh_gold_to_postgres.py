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

    sparkjob dwh_gold_to_postgres.py            # nạp + khai khoá
    sparkjob dwh_gold_to_postgres.py --no-keys  # chỉ nạp
    sparkjob dwh_gold_to_postgres.py --only fact_transactions

Mỗi bảng ghi bằng ``mode("overwrite")`` nên chạy lại là idempotent: Spark DROP rồi
CREATE lại.

Vì sao có bước khai khoá
------------------------
Chính vì Spark DROP/CREATE: bảng nó tạo KHÔNG có primary key, foreign key hay
index. Mà công cụ vẽ ER diagram (DBeaver, DataGrip, pgAdmin) dựng quan hệ từ
**foreign key trong catalog** — không có FK thì diagram chỉ là mấy cái hộp rời
nhau, không thấy hình sao. Nên sau khi nạp, ``declare_keys`` khai:

  * PK trên ``id`` của 4 dim
  * FK từ ``fact_transactions.{user,card,merchant,device}_id`` sang dim tương ứng
  * index trên 4 cột FK của fact (Postgres KHÔNG tự tạo index cho phía con)

Khoá bị mất mỗi lần nạp lại (DROP TABLE cuốn theo constraint), nên bước này chạy
liền sau bước nạp trong cùng một lần chạy. Và vì FK của lần trước sẽ CHẶN việc
DROP dim ở lần sau, ``drop_keys`` phải chạy TRƯỚC vòng ghi — thứ tự
``drop_keys -> ghi -> declare_keys`` là bắt buộc, không tuỳ ý.
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


def _connect(autocommit: bool = False) -> psycopg.Connection:
    """Kết nối psycopg tới cùng database mà JDBC ghi vào.

    Cần một đường ngoài JDBC vì Spark chỉ ghi được DỮ LIỆU: tạo schema và khai
    khoá đều là DDL nằm ngoài khả năng của writer.
    """
    _, props = jdbc_props()
    host = os.environ.get("PG_HOST", "postgres")
    db = os.environ.get("PG_DB", "warehouse")
    return psycopg.connect(
        f"host={host} port=5432 dbname={db} "
        f"user={props['user']} password={props['password']}",
        autocommit=autocommit)


def ensure_schema(schema: str) -> None:
    """Tạo schema nếu chưa có.

    Writer JDBC của Spark tạo được BẢNG nhưng không tạo SCHEMA — thiếu bước này
    thì lỗi là ``schema "dwh" does not exist`` ngay ở bảng đầu tiên.
    """
    with _connect(autocommit=True) as conn:
        conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')


def drop_keys(schema: str) -> None:
    """Gỡ mọi FK trong schema TRƯỚC khi nạp lại.

    Bắt buộc, không phải dọn dẹp cho đẹp: Spark ghi bằng ``mode("overwrite")``,
    tức DROP TABLE rồi CREATE. Postgres từ chối DROP một bảng đang được FK trỏ
    tới, nên lần nạp THỨ HAI chết ngay ở dim đầu tiên::

        cannot drop table dwh.dim_user because other objects depend on it
        Detail: constraint fact_user_id_fk on table dwh.fact_transactions ...

    Gỡ trước thì mọi lần nạp đều giống lần đầu. Chỉ cần gỡ FK: PK không cản DROP
    bảng của chính nó, và bảng bị drop thì PK đi theo.

    Chạy KỂ CẢ khi ``--no-keys``: FK có thể còn sót từ lần chạy trước.
    """
    with _connect(autocommit=True) as conn:
        rows = conn.execute("""
            SELECT n.nspname, t.relname, c.conname
            FROM pg_constraint c
            JOIN pg_class t     ON t.oid = c.conrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            WHERE c.contype = 'f' AND n.nspname = %s
        """, (schema,)).fetchall()
        for ns, tbl, name in rows:
            conn.execute(f'ALTER TABLE "{ns}"."{tbl}" DROP CONSTRAINT "{name}"')
        if rows:
            print(f"[DWH] gỡ {len(rows)} FK cũ trước khi nạp lại")


def declare_keys(schema: str) -> None:
    """Khai PK + FK + index để công cụ BI dựng được ER diagram.

    Ba việc, theo đúng thứ tự bắt buộc: PK của dim trước (FK phải trỏ tới một khoá
    duy nhất), rồi FK của fact, rồi index cho cột FK.

    Hai ca dữ liệu được kiểm TRƯỚC khi khai, vì cả hai đều làm lệnh ALTER chết
    giữa chừng và để lại schema khai dở:

    * **dim có nhiều version một ``id``** (SCD2 đã merge ít nhất một lần) -> không
      đặt PK trên ``id`` được. Dừng và nói rõ, vì lúc đó star schema cần surrogate
      key chứ không phải natural key — sửa bằng cách nào là quyết định mô hình,
      không phải thứ script tự đoán thay.
    * **fact có khoá mồ côi** (trỏ tới ``id`` không có trong dim) -> vẫn khai FK
      nhưng ở dạng ``NOT VALID`` để diagram vẽ được, KÈM cảnh báo số dòng mồ côi.
      Im lặng bỏ qua thì diagram trông đẹp trong khi dữ liệu đang hỏng.
    """
    fks = {"user_id": "dim_user", "card_id": "dim_card",
           "merchant_id": "dim_merchant", "device_id": "dim_device"}
    with _connect(autocommit=True) as conn:
        def exists(t: str) -> bool:
            return conn.execute("SELECT to_regclass(%s)", (f"{schema}.{t}",)).fetchone()[0] is not None

        dims = [d for d in fks.values() if exists(d)]
        for d in dims:
            dup = conn.execute(
                f'SELECT count(*) - count(DISTINCT id) FROM "{schema}"."{d}"').fetchone()[0]
            if dup:
                raise SystemExit(
                    f"[DWH] {schema}.{d} có {dup:,} dòng trùng id — dim đang giữ nhiều "
                    f"version SCD2 nên không đặt được PK trên id.\n"
                    f"      Star schema lúc này cần surrogate key, hoặc một bảng "
                    f"dim_*_current riêng để fact trỏ vào. Không tự chọn thay bạn.")
            conn.execute(f'ALTER TABLE "{schema}"."{d}" '
                         f'DROP CONSTRAINT IF EXISTS "{d}_pk"')
            conn.execute(f'ALTER TABLE "{schema}"."{d}" '
                         f'ADD CONSTRAINT "{d}_pk" PRIMARY KEY (id)')
            print(f"[DWH] PK  {schema}.{d}(id)")

        if not exists("fact_transactions"):
            return
        for col, dim in fks.items():
            if dim not in dims:
                print(f"[DWH] bỏ qua FK {col} — thiếu {schema}.{dim}")
                continue
            orphan = conn.execute(
                f'SELECT count(*) FROM "{schema}".fact_transactions f '
                f'LEFT JOIN "{schema}"."{dim}" d ON d.id = f."{col}" '
                f'WHERE f."{col}" IS NOT NULL AND d.id IS NULL').fetchone()[0]
            name = f"fact_{col}_fk"
            conn.execute(f'ALTER TABLE "{schema}".fact_transactions '
                         f'DROP CONSTRAINT IF EXISTS "{name}"')
            conn.execute(f'ALTER TABLE "{schema}".fact_transactions '
                         f'ADD CONSTRAINT "{name}" FOREIGN KEY ("{col}") '
                         f'REFERENCES "{schema}"."{dim}"(id)'
                         + (" NOT VALID" if orphan else ""))
            conn.execute(f'CREATE INDEX IF NOT EXISTS "fact_{col}_idx" '
                         f'ON "{schema}".fact_transactions ("{col}")')
            flag = f"  ⚠ {orphan:,} dòng mồ côi -> NOT VALID" if orphan else ""
            print(f"[DWH] FK  fact_transactions.{col} -> {dim}.id{flag}")


def row_counts(schema: str, tables: list[str]) -> dict[str, int]:
    """Đếm dòng SAU khi ghi, hỏi thẳng Postgres.

    Không dùng ``df.count()``: đó là một action Spark nữa, quét lại parquet trên
    GCS chỉ để in một con số. Hỏi đích đến vừa rẻ hơn vừa chứng minh dữ liệu đã
    thực sự nằm trong bảng.
    """
    out = {}
    with _connect() as conn:
        for t in tables:
            out[t] = conn.execute(f'SELECT count(*) FROM "{schema}"."{t}"').fetchone()[0]
    return out


def main() -> None:
    p = argparse.ArgumentParser(description="DEMO: Gold -> Postgres star schema")
    p.add_argument("--schema", default="dwh", help="schema đích trong database warehouse")
    p.add_argument("--only", help="chỉ ghi các bảng này (phân tách bằng dấu phẩy)")
    p.add_argument("--no-keys", action="store_true",
                   help="Chỉ nạp, không khai PK/FK. Diagram sẽ không có quan hệ.")
    args = p.parse_args()

    tables = args.only.split(",") if args.only else TABLES
    unknown = [t for t in tables if t not in TABLES]
    if unknown:
        raise SystemExit(f"không biết bảng {unknown}; hợp lệ: {TABLES}")

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    ensure_schema(args.schema)
    drop_keys(args.schema)          # phải TRƯỚC vòng ghi, xem docstring drop_keys
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

    # Sau bước nạp, vì overwrite = DROP TABLE nên constraint của lần trước đã mất.
    if not args.no_keys:
        declare_keys(args.schema)

    print(f"\n=== {args.schema} ===")
    for t, n in row_counts(args.schema, tables).items():
        print(f"  {t:<20} {n:>9,} dòng")

    spark.stop()


if __name__ == "__main__":
    main()
