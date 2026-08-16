"""Tiện ích I/O data lake (Cloud Storage) dùng chung cho các pipeline.

Dùng ``pyarrow.fs`` — có sẵn trong image, không cần google-cloud-storage. Credential
lấy qua ADC (service account của VM), xem ``include/lake.py``.

Tham số ``layer`` của các hàm dưới đây là **tên tầng medallion** (``source`` /
``raw`` / ``staging`` / ``curated``). ``lake.path()`` biến nó thành đường dẫn thật,
nên tầng gọi hàm không cần biết bốn tầng đó là 4 bucket riêng hay 4 prefix trong
một bucket.
"""

from __future__ import annotations

import pyarrow.dataset as pads
import pyarrow.fs as pafs

from include import lake

# Dataset partition theo event_date (đọc kiểu hive); còn lại là snapshot 1 file.
PARTITIONED = {"transactions"}


def get_lake_fs() -> pafs.GcsFileSystem:
    """FileSystem của data lake (GCS qua ADC)."""
    return lake.filesystem()


def _dataset(fs: pafs.GcsFileSystem, layer: str, dataset: str) -> pads.Dataset:
    """Mở pyarrow Dataset cho 1 dataset (partition với transactions, snapshot với dim)."""
    return pads.dataset(
        lake.path(layer, dataset), filesystem=fs, format="parquet",
        partitioning="hive" if dataset in PARTITIONED else None,
    )


def list_files(fs: pafs.GcsFileSystem, prefix: str) -> list[str]:
    """Liệt kê mọi file (không kể 'thư mục') dưới ``prefix`` = 'bucket/key...'.

    ``prefix`` là đường dẫn ĐÃ qua ``lake.path()``, không phải tên tầng.
    """
    sel = pafs.FileSelector(prefix, recursive=True)
    return [f.path for f in fs.get_file_info(sel) if f.type == pafs.FileType.File]


def _copy_tree(fs: pafs.GcsFileSystem, src_layer: str, dst_layer: str, rel_path: str) -> int:
    """Copy mọi file dưới ``<src_layer>/<rel_path>`` sang ``<dst_layer>/<rel_path>``.

    Xoá đích trước để chạy lại là idempotent. Trả số file đã copy.

    LƯU Ý HIỆU NĂNG: ``copy_file`` của pyarrow kéo bytes QUA MÁY đang
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


def copy_dataset(fs: pafs.GcsFileSystem, src_layer: str, dst_layer: str, dataset: str) -> int:
    """Copy nguyên xi 1 dataset từ tầng ``src_bucket`` sang ``dst_bucket``.

    Bronze giữ data thô nên copy y hệt bytes. Dùng cho bootstrap / full-load;
    DP1 daily dùng ``copy_partition``.
    """
    return _copy_tree(fs, src_layer, dst_layer, dataset)


def copy_partition(fs: pafs.GcsFileSystem, src_layer: str, dst_layer: str,
                   dataset: str, event_date: str) -> int:
    """Copy 1 partition ``event_date`` của ``dataset`` (DP1 daily incremental).

    Trả số file đã copy (0 nếu ngày đó không có data trong source).
    """
    return _copy_tree(fs, src_layer, dst_layer, f"{dataset}/event_date={event_date}")


def row_count(fs: pafs.GcsFileSystem, layer: str, dataset: str) -> int:
    """Đếm tổng số dòng của 1 dataset."""
    return _dataset(fs, layer, dataset).count_rows()


def columns(fs: pafs.GcsFileSystem, layer: str, dataset: str) -> set[str]:
    """Tập tên cột (schema fragment đầu tiên) của 1 dataset."""
    return set(_dataset(fs, layer, dataset).schema.names)
