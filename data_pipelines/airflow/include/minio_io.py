"""Tiện ích I/O data lake dùng chung cho các data pipeline.

Hai backend, cùng một API — chọn bằng ``LAKE_ROOT`` (xem ``include/lake.py``):

* **MinIO (local)** — cấu hình kết nối lấy từ **Airflow Connection**
  ``minio_default`` (không hardcode secret; thoả yêu cầu "connection để trong
  Airflow, reuse across pipelines").
* **GCS (trên GCP)** — Application Default Credentials, tức service account gắn
  trên VM. Không có Airflow Connection nào để đọc, nên đường này đi qua
  ``lake.filesystem()``.

Dùng ``pyarrow.fs`` (có sẵn trong image, không cần boto3/s3fs/google-cloud-storage)
để list / copy object và đếm dòng / đọc schema Parquet.

Tham số ``bucket`` của các hàm dưới đây thực chất là **tên tầng medallion**
(``source`` / ``raw`` / ``staging`` / ``curated``). ``lake.path()`` biến nó thành
đường dẫn thật, nên tầng gọi hàm không cần biết bốn tầng đó là 4 bucket riêng hay
4 prefix trong một bucket.
"""

from __future__ import annotations

import pyarrow.dataset as pads
import pyarrow.fs as pafs

from include import lake

# Dataset partition theo event_date (đọc kiểu hive); còn lại là snapshot 1 file.
PARTITIONED = {"transactions"}


def get_lake_fs(conn_id: str = "minio_default") -> pafs.FileSystem:
    """pyarrow FileSystem cho data lake.

    GCS: đi thẳng qua ``lake.filesystem()`` (ADC).
    MinIO: ưu tiên Airflow Connection; nếu không có (chạy ngoài context Airflow)
    thì rơi về biến môi trường.
    """
    if lake.is_gcs():
        return lake.filesystem()
    try:
        from airflow.sdk import Connection

        c = Connection.get(conn_id)
    except Exception:
        return lake.filesystem()
    extra = c.extra_dejson or {}
    endpoint = f"{c.host}:{c.port}" if c.port else c.host
    return pafs.S3FileSystem(
        access_key=c.login, secret_key=c.password,
        endpoint_override=endpoint, scheme=extra.get("scheme", "http"),
        allow_bucket_creation=True,
    )


# Tên cũ — giữ lại để không phải sửa mọi chỗ gọi.
get_s3fs = get_lake_fs


def _dataset(fs: pafs.FileSystem, bucket: str, dataset: str) -> pads.Dataset:
    """Mở pyarrow Dataset cho 1 dataset (partition với transactions, snapshot với dim)."""
    return pads.dataset(
        lake.path(bucket, dataset), filesystem=fs, format="parquet",
        partitioning="hive" if dataset in PARTITIONED else None,
    )


def list_files(fs: pafs.FileSystem, prefix: str) -> list[str]:
    """Liệt kê mọi file (không kể 'thư mục') dưới ``prefix`` = 'bucket/key...'.

    ``prefix`` là đường dẫn ĐÃ qua ``lake.path()``, không phải tên tầng.
    """
    sel = pafs.FileSelector(prefix, recursive=True)
    return [f.path for f in fs.get_file_info(sel) if f.type == pafs.FileType.File]


def _copy_tree(fs: pafs.FileSystem, src_layer: str, dst_layer: str, rel_path: str) -> int:
    """Copy mọi file dưới ``<src_layer>/<rel_path>`` sang ``<dst_layer>/<rel_path>``.

    Xoá đích trước để chạy lại là idempotent. Trả số file đã copy.

    LƯU Ý HIỆU NĂNG trên GCS: ``copy_file`` của pyarrow kéo bytes QUA MÁY đang
    chạy. Với nhịp hằng ngày (1 file ~700 KB) thì không đáng kể, nhưng backfill
    368 partition sẽ tải xuống rồi đẩy lên lại ~262 MB một cách vô ích — lúc đó
    nên dùng server-side rewrite của GCS (``gcloud storage cp``) thay vì hàm này.
    """
    src_dir = lake.path(src_layer, rel_path)
    dst_dir = lake.path(dst_layer, rel_path)
    try:
        fs.delete_dir_contents(dst_dir, missing_dir_ok=True)
    except (FileNotFoundError, OSError):
        pass
    n = 0
    for src_path in list_files(fs, src_dir):
        rel = src_path[len(src_dir):].lstrip("/")   # key tương đối trong dataset
        fs.copy_file(src_path, f"{dst_dir}/{rel}")  # copy giữ nguyên bytes
        n += 1
    return n


def copy_dataset(fs: pafs.FileSystem, src_bucket: str, dst_bucket: str, dataset: str) -> int:
    """Copy nguyên xi 1 dataset từ tầng ``src_bucket`` sang ``dst_bucket``.

    Bronze giữ data thô nên copy y hệt bytes. Dùng cho bootstrap / full-load;
    DP1 daily dùng ``copy_partition``.
    """
    return _copy_tree(fs, src_bucket, dst_bucket, dataset)


def copy_partition(fs: pafs.FileSystem, src_bucket: str, dst_bucket: str,
                   dataset: str, event_date: str) -> int:
    """Copy 1 partition ``event_date`` của ``dataset`` (DP1 daily incremental).

    Trả số file đã copy (0 nếu ngày đó không có data trong source).
    """
    return _copy_tree(fs, src_bucket, dst_bucket, f"{dataset}/event_date={event_date}")


def row_count(fs: pafs.FileSystem, bucket: str, dataset: str) -> int:
    """Đếm tổng số dòng của 1 dataset."""
    return _dataset(fs, bucket, dataset).count_rows()


def columns(fs: pafs.FileSystem, bucket: str, dataset: str) -> set[str]:
    """Tập tên cột (schema fragment đầu tiên) của 1 dataset."""
    return set(_dataset(fs, bucket, dataset).schema.names)
