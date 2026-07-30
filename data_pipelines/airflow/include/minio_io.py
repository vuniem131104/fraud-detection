"""Tiện ích I/O MinIO (S3) dùng chung cho các data pipeline.

Đọc cấu hình kết nối từ **Airflow Connection** ``minio_default`` (không hardcode
secret trong code — thoả yêu cầu "connection để trong Airflow, reuse across
pipelines"). Dùng ``pyarrow.fs.S3FileSystem`` (có sẵn trong image, không cần
boto3/s3fs) để list / copy object và đếm dòng / đọc schema Parquet.
"""

from __future__ import annotations

import pyarrow.dataset as pads
import pyarrow.fs as pafs
from airflow.sdk import Connection

# Dataset partition theo event_date (đọc kiểu hive); còn lại là snapshot 1 file.
PARTITIONED = {"transactions"}


def get_s3fs(conn_id: str = "minio_default") -> pafs.S3FileSystem:
    """Tạo pyarrow S3FileSystem từ Airflow Connection ``conn_id``."""
    c = Connection.get(conn_id)
    extra = c.extra_dejson or {}
    endpoint = f"{c.host}:{c.port}" if c.port else c.host
    return pafs.S3FileSystem(
        access_key=c.login, secret_key=c.password,
        endpoint_override=endpoint, scheme=extra.get("scheme", "http"),
        allow_bucket_creation=True,
    )


def _dataset(fs: pafs.S3FileSystem, bucket: str, dataset: str) -> pads.Dataset:
    """Mở pyarrow Dataset cho 1 dataset (partition với transactions, snapshot với dim)."""
    return pads.dataset(
        f"{bucket}/{dataset}", filesystem=fs, format="parquet",
        partitioning="hive" if dataset in PARTITIONED else None,
    )


def list_files(fs: pafs.S3FileSystem, prefix: str) -> list[str]:
    """Liệt kê mọi file (không kể 'thư mục') dưới ``prefix`` = 'bucket/key...'."""
    sel = pafs.FileSelector(prefix, recursive=True)
    return [f.path for f in fs.get_file_info(sel) if f.type == pafs.FileType.File]


def copy_dataset(fs: pafs.S3FileSystem, src_bucket: str, dst_bucket: str, dataset: str) -> int:
    """Copy nguyên xi 1 dataset từ ``src_bucket`` sang ``dst_bucket`` (Bronze giữ data thô).

    Xoá sạch đích trước để idempotent khi chạy lại. Trả về số file đã copy.
    Dùng cho bootstrap / full-load; DP1 daily dùng ``copy_partition``.
    """
    dst_dir = f"{dst_bucket}/{dataset}"
    try:
        fs.delete_dir_contents(dst_dir, missing_dir_ok=True)
    except (FileNotFoundError, OSError):
        pass
    n = 0
    for src_path in list_files(fs, f"{src_bucket}/{dataset}"):
        rel = src_path[len(src_bucket) + 1:]          # key sau tên bucket
        fs.copy_file(src_path, f"{dst_bucket}/{rel}")  # copy giữ nguyên bytes
        n += 1
    return n


def copy_partition(fs: pafs.S3FileSystem, src_bucket: str, dst_bucket: str,
                   dataset: str, event_date: str) -> int:
    """Copy 1 partition ``event_date`` của ``dataset`` (dùng cho DP1 daily incremental).

    Xoá partition đích trước để chạy lại idempotent. Trả số file đã copy (0 nếu
    ngày đó không có data trong source).
    """
    part = f"{dataset}/event_date={event_date}"
    dst_dir = f"{dst_bucket}/{part}"
    try:
        fs.delete_dir_contents(dst_dir, missing_dir_ok=True)
    except (FileNotFoundError, OSError):
        pass
    n = 0
    for src_path in list_files(fs, f"{src_bucket}/{part}"):
        rel = src_path[len(src_bucket) + 1:]
        fs.copy_file(src_path, f"{dst_bucket}/{rel}")
        n += 1
    return n


def row_count(fs: pafs.S3FileSystem, bucket: str, dataset: str) -> int:
    """Đếm tổng số dòng của 1 dataset."""
    return _dataset(fs, bucket, dataset).count_rows()


def columns(fs: pafs.S3FileSystem, bucket: str, dataset: str) -> set[str]:
    """Tập tên cột (schema fragment đầu tiên) của 1 dataset."""
    return set(_dataset(fs, bucket, dataset).schema.names)
