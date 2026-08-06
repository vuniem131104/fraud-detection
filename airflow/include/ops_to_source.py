"""DP0 — job EXPORT của team data: ``ops.*`` (Cloud SQL) -> GCS ``source``.

Mô phỏng bước "team data đóng gói dữ liệu trong ngày ra file cho các team khác
kéo về". Đây là **ranh giới tổ chức** duy nhất trong luồng: từ đây trở đi team ML
chỉ thấy file, không truy cập cơ sở dữ liệu vận hành.

Xuất hai loại dữ liệu, hai nhịp khác nhau:

  ``transactions``  fact  -> partition theo ngày, mỗi ngày một file (lịch sử tích luỹ)
  4 bảng reference  dim   -> FULL SNAPSHOT, ghi đè mỗi ngày

Vì sao dim phải là full snapshot mỗi ngày
-----------------------------------------
DP2 dựng SCD Type 2 bằng cách so snapshot hôm nay với bản ``is_current`` đang có ở
Gold. Không có snapshot mới thì không có gì để so, và ``valid_from_ts`` /
``valid_to_ts`` / ``is_current`` tồn tại mà không bao giờ đổi. Snapshot 25k user
chỉ ~1 MB nên đây là cách rẻ và đúng.

Vì sao nguồn là BẢNG chứ không phải Kafka
-----------------------------------------
Chạy lại bao nhiêu lần cũng ra kết quả y hệt (query theo khoảng ``created_at``), và
không phụ thuộc retention của Kafka. Duplicate mà stream tiêm vào đã nằm sẵn trong
bảng (``ops.transactions`` cố ý không có PK) nên nó đi thẳng vào file -> Bronze
giữ thô -> DP2 khử.

Chạy::

    python -m include.ops_to_source --date 2026-07-28                  # daily
    python -m include.ops_to_source --from 2025-07-27 --to 2026-07-28  # backfill lịch sử
    python -m include.ops_to_source --dims-only                        # chỉ refresh dim
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta

import pandas as pd
import psycopg
import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq

try:                        # chạy như package (python -m include.ops_to_source)
    from include import lake
except ImportError:        # chạy trực tiếp trong thư mục include/
    import lake

# Mốc schema evolution: partition TRƯỚC ngày này không có cột auth_3ds_flag.
CUTOVER = os.environ.get("SCHEMA_CUTOVER_DATE", "2026-05-01")
NEW_COL = "auth_3ds_flag"
LOCAL_TZ = "Asia/Ho_Chi_Minh"

TX_COLS = ["id", "user_id", "card_id", "merchant_id", "device_id", "amount_usd",
           "currency", "channel", "billing_country_code", "ip_country_code",
           "email_purchaser", "email_recipient", "created_at", NEW_COL]

# bảng reference -> cột export. Bỏ ``updated_at``: đó là metadata nội bộ của team
# data, và nếu export thì hash SCD2 sẽ đổi mỗi ngày dù thuộc tính nghiệp vụ không
# đổi -> sinh version rác.
DIMS = {
    "users": ["id", "email", "country_code", "customer_segment",
              "kyc_level", "email_verified", "created_at"],
    "cards": ["id", "user_id", "issuer_code", "country_code", "brand",
              "type", "bin_code", "is_virtual", "created_at"],
    "merchants": ["id", "name", "category", "country_code", "risk_level", "created_at"],
    "devices": ["id", "fingerprint", "device_type", "os", "browser",
                "screen_resolution", "created_at"],
}


def ops_dsn() -> str:
    """DSN tới DB vận hành (opsdb)."""
    return (f"host={os.environ.get('PG_HOST', 'postgres')} "
            f"port={os.environ.get('POSTGRES_PORT', '5432')} "
            f"dbname={os.environ.get('OPS_POSTGRES_DB', 'opsdb')} "
            f"user={os.environ.get('POSTGRES_USER') or os.environ['AIRFLOW_USER']} "
            f"password={os.environ.get('POSTGRES_PASSWORD') or os.environ['AIRFLOW_PASSWORD']}")


def get_lake_fs() -> tuple[pafs.GcsFileSystem, str]:
    """FileSystem của data lake + đường dẫn tầng ``source`` (đã resolve).

    Giá trị thứ hai là đường dẫn ĐÃ qua ``lake.path()``, nên mọi chỗ dùng
    ``f"{bucket}/..."`` bên dưới không cần biết layout bucket.
    """
    layer = os.environ.get("SOURCE_BUCKET", "source")
    return lake.filesystem(), lake.path(layer)


def _write_parquet(fs: pafs.GcsFileSystem, path: str, df: pd.DataFrame) -> None:
    """Ghi 1 file parquet.

    ``coerce_timestamps='us'``: Spark 3.5 không đọc được TIMESTAMP(NANOS), mà pandas
    mặc định dùng nanosecond — không ép thì DP2 lỗi ``Illegal Parquet type``.
    """
    with fs.open_output_stream(path) as out:
        pq.write_table(pa.Table.from_pandas(df, preserve_index=False), out,
                       coerce_timestamps="us", allow_truncated_timestamps=True)


# --------------------------------------------------------------------------- #
# transactions (fact, partition theo ngày)                                     #
# --------------------------------------------------------------------------- #

def _prepare_tx(df: pd.DataFrame, date_str: str) -> pd.DataFrame:
    """Áp schema evolution + ép kiểu cho một partition ngày."""
    if date_str < CUTOVER:
        # hệ nguồn chưa có cột này -> BỎ HẲN khỏi file (không phải để null)
        return df.drop(columns=[NEW_COL], errors="ignore")
    if NEW_COL in df.columns:
        # nếu cả cột đều NULL, pyarrow suy ra type `null` -> Spark mergeSchema fail
        # khi gộp với partition khác (boolean). Ép nullable boolean cho chắc.
        df = df.copy()
        df[NEW_COL] = df[NEW_COL].astype("boolean")
    return df


def write_tx_partition(fs: pafs.GcsFileSystem, bucket: str, date_str: str,
                       df: pd.DataFrame) -> None:
    """Ghi 1 partition ngày (xoá trước để chạy lại idempotent)."""
    part_dir = f"{bucket}/transactions/event_date={date_str}"
    try:
        fs.delete_dir_contents(part_dir, missing_dir_ok=True)
    except (FileNotFoundError, OSError):
        pass
    _write_parquet(fs, f"{part_dir}/part-0.parquet", _prepare_tx(df, date_str))


def export_day(date_str: str, quiet: bool = False) -> int:
    """Export transactions của MỘT ngày. Trả số dòng đã ghi."""
    start = datetime.fromisoformat(date_str)
    end = start + timedelta(days=1)
    sql = (f"SELECT {','.join(TX_COLS)} FROM ops.transactions "
           "WHERE created_at >= %s AND created_at < %s ORDER BY created_at")
    with psycopg.connect(ops_dsn()) as conn:
        df = pd.read_sql(sql, conn, params=(start, end))
    if df.empty:
        if not quiet:
            print(f"[dp0] {date_str}: không có giao dịch nào trong ops.transactions")
        return 0

    df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
    fs, bucket = get_lake_fs()
    write_tx_partition(fs, bucket, date_str, df)

    if not quiet:
        n_uni = df["id"].nunique()
        print(f"[dp0] {date_str}: {len(df):,} dòng -> "
              f"{lake.lake_root()}{bucket}/transactions/event_date={date_str}/part-0.parquet")
        print(f"      duplicate={len(df) - n_uni:,} ({1 - n_uni / len(df):.2%}) | "
              f"US={(df['billing_country_code'] == 'US').mean():.1%} | "
              f"card distinct={df['card_id'].nunique():,} | "
              f"{NEW_COL}={'có' if date_str >= CUTOVER else 'BỎ (trước cutover)'}")
    return len(df)


def export_range(from_date: str, to_date: str) -> int:
    """Export nhiều ngày liên tiếp [from, to] — dùng khi nạp lịch sử.

    Đọc CẢ KHOẢNG bằng MỘT query rồi group theo ngày trong bộ nhớ. Gọi
    ``export_day`` 367 lần sẽ mở 367 connection + 367 lần quét bảng: chậm tới mức
    task Airflow timeout (đã đo: >10 phút).
    """
    start = datetime.fromisoformat(from_date)
    end = datetime.fromisoformat(to_date) + timedelta(days=1)
    sql = (f"SELECT {','.join(TX_COLS)} FROM ops.transactions "
           "WHERE created_at >= %s AND created_at < %s")
    print(f"[dp0] đọc ops.transactions {from_date}..{to_date} (1 query) ...")
    with psycopg.connect(ops_dsn()) as conn:
        df = pd.read_sql(sql, conn, params=(start, end))

    df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
    # event_date theo giờ ĐỊA PHƯƠNG để khớp partition của luồng live
    df["event_date"] = df["created_at"].dt.tz_convert(LOCAL_TZ).dt.strftime("%Y-%m-%d")
    print(f"      {len(df):,} dòng, {df['event_date'].nunique()} ngày")

    fs, bucket = get_lake_fs()
    n_days = n_rows = 0
    for ds, part in df.groupby("event_date", sort=True):
        write_tx_partition(fs, bucket, ds, part.drop(columns=["event_date"]))
        n_days += 1
        n_rows += len(part)
        if n_days % 60 == 0:
            print(f"      ... {n_days} ngày, {n_rows:,} dòng (tới {ds})")
    print(f"[dp0] transactions: {n_days} ngày, {n_rows:,} dòng "
          f"-> {lake.lake_root()}{bucket}/transactions/")
    return n_rows


# --------------------------------------------------------------------------- #
# reference data (dim, full snapshot)                                          #
# --------------------------------------------------------------------------- #

def export_dims(quiet: bool = False) -> dict[str, int]:
    """Export FULL SNAPSHOT 4 bảng reference data (ghi đè bản hôm trước)."""
    fs, bucket = get_lake_fs()
    counts = {}
    with psycopg.connect(ops_dsn()) as conn:
        for name, cols in DIMS.items():
            df = pd.read_sql(f"SELECT {','.join(cols)} FROM ops.{name} ORDER BY id", conn)
            for c in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[c]):
                    df[c] = pd.to_datetime(df[c], utc=True)
            _write_parquet(fs, f"{bucket}/{name}/snapshot.parquet", df)
            counts[name] = len(df)
    if not quiet:
        print("[dp0] dim snapshot -> %s%s/: %s"
              % (lake.lake_root(), bucket,
                 "  ".join(f"{k}={v:,}" for k, v in counts.items())))
    return counts


# --------------------------------------------------------------------------- #
# CLI                                                                          #
# --------------------------------------------------------------------------- #

def build_parser() -> argparse.ArgumentParser:
    """CLI: 1 ngày (--date), cả khoảng (--from/--to), hoặc chỉ dim (--dims-only)."""
    p = argparse.ArgumentParser(description="DP0 export ops.* -> GCS source")
    p.add_argument("--date", help="YYYY-MM-DD (1 ngày)")
    p.add_argument("--from", dest="from_date", help="YYYY-MM-DD (đầu khoảng)")
    p.add_argument("--to", dest="to_date", help="YYYY-MM-DD (cuối khoảng, inclusive)")
    p.add_argument("--dims-only", action="store_true",
                   help="Chỉ export lại 4 bảng reference data.")
    p.add_argument("--no-dims", action="store_true",
                   help="Bỏ qua reference data (chỉ transactions).")
    return p


def main() -> int:
    """Export dim (trừ khi --no-dims) rồi transactions theo chế độ đã chọn."""
    a = build_parser().parse_args()
    if a.dims_only:
        export_dims()
        return 0
    if not (a.date or (a.from_date and a.to_date)):
        raise SystemExit("cần --date, hoặc (--from và --to), hoặc --dims-only")

    if not a.no_dims:
        export_dims()
    if a.from_date and a.to_date:
        export_range(a.from_date, a.to_date)
    else:
        export_day(a.date)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
