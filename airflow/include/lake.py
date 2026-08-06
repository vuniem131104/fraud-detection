"""Data lake trên Cloud Storage — đường dẫn và filesystem dùng chung.

``LAKE_ROOT`` quyết định layout, hai cách đặt đều chạy mà code không cần biết:

========================  ==========================  ==============================
LAKE_ROOT                 Đường dẫn pyarrow           URI cho Spark
========================  ==========================  ==============================
``gs://my-lake/``         ``my-lake/raw/transactions``  ``gs://my-lake/raw/transa...``
``gs://``                 ``raw/transactions``          ``gs://raw/transactions``
========================  ==========================  ==============================

Bốn tầng medallion (``source`` / ``raw`` / ``staging`` / ``curated``) đứng ngay sau
``LAKE_ROOT``, nên để ``gs://<bucket>/`` thì chúng thành PREFIX trong một bucket,
còn để ``gs://`` thì thành TÊN BUCKET.

Vì sao tách khỏi ``lake_io.py``: module này KHÔNG import airflow, nên
``ops_to_source.py`` (chạy bằng ``python -m``, có thể ngoài context Airflow) dùng
được.

Credential: Application Default Credentials — trên VM GCP đó là service account
gắn kèm, không cần key file. pyarrow đọc bằng C++ nên không cần ``google-auth``.
"""

from __future__ import annotations

import os

import pyarrow.fs as pafs


def lake_root() -> str:
    """``LAKE_ROOT`` đã chuẩn hoá (luôn kết thúc bằng ``/``).

    Không có giá trị mặc định: sai biến này thì mọi job đọc/ghi sai chỗ mà không
    báo lỗi, nên fail sớm và rõ ràng hơn là đoán.
    """
    root = os.environ.get("LAKE_ROOT")
    if not root:
        raise RuntimeError("Thiếu LAKE_ROOT (ví dụ: gs://my-lake/)")
    if not root.startswith("gs://"):
        raise RuntimeError(f"LAKE_ROOT phải bắt đầu bằng gs:// — đang là {root!r}")
    return root if root.endswith("/") else root + "/"


def _root_prefix() -> str:
    """Phần sau ``gs://``: ``""`` (bucket-per-layer) hoặc ``"my-lake/"``."""
    return lake_root()[len("gs://"):]


def path(layer: str, *parts: str) -> str:
    """Đường dẫn kiểu pyarrow (KHÔNG có scheme): ``my-lake/raw/transactions``.

    ``layer`` là tên tầng medallion. ``GcsFileSystem`` của pyarrow nhận đường dẫn
    dạng ``bucket/key...``, khác Spark vốn cần scheme đầy đủ.
    """
    joined = "/".join(p.strip("/") for p in (layer, *parts) if p)
    return f"{_root_prefix()}{joined}"


def uri(layer: str, *parts: str) -> str:
    """URI đầy đủ cho Spark: ``gs://my-lake/raw/transactions``."""
    joined = "/".join(p.strip("/") for p in (layer, *parts) if p)
    return f"{lake_root()}{joined}"


def filesystem() -> pafs.GcsFileSystem:
    """pyarrow GcsFileSystem dùng ADC.

    Bucket phải tạo trước bằng Terraform/gcloud — tạo bucket là việc của hạ tầng,
    không phải của pipeline.
    """
    return pafs.GcsFileSystem()
