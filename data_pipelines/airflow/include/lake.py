"""Data lake: MinIO (local) hoặc GCS (trên GCP) — cùng một API.

Toàn bộ khác biệt gói trong MỘT biến môi trường ``LAKE_ROOT``:

===========================  =========================  =========================
LAKE_ROOT                    Đường dẫn pyarrow          URI cho Spark
===========================  =========================  =========================
``s3a://`` (mặc định)        ``raw/transactions``       ``s3a://raw/transactions``
``gs://my-lake/``            ``my-lake/raw/transa...``  ``gs://my-lake/raw/tra...``
``gs://`` (4 bucket riêng)   ``raw/transactions``       ``gs://raw/transactions``
===========================  =========================  =========================

Vì sao MỘT biến mà đủ cho cả hai cách tổ chức bucket: bốn tầng medallion
(``source`` / ``raw`` / ``staging`` / ``curated``) đứng ngay sau ``LAKE_ROOT``, nên
để ``gs://<bucket>/`` thì chúng thành PREFIX trong một bucket, còn để ``gs://``
thì chúng thành TÊN BUCKET. Không chỗ nào trong code cần biết bạn chọn kiểu nào.

Vì sao tách khỏi ``minio_io.py``: module này KHÔNG import airflow, nên
``ops_to_source.py`` (chạy bằng ``python -m``, có thể ngoài context Airflow) dùng
được mà không cần Airflow Connection.

Credential:
  * GCS  — Application Default Credentials. Trên VM GCP đó là service account gắn
    kèm, không cần key file. pyarrow đọc bằng C++ nên không cần ``google-auth``.
  * MinIO — ``MINIO_ROOT_USER`` / ``MINIO_ROOT_PASSWORD`` + ``MINIO_ENDPOINT``.
"""

from __future__ import annotations

import os

import pyarrow.fs as pafs

DEFAULT_LAKE_ROOT = "s3a://"


def lake_root() -> str:
    """``LAKE_ROOT`` đã chuẩn hoá (luôn kết thúc bằng ``/``)."""
    root = os.environ.get("LAKE_ROOT") or DEFAULT_LAKE_ROOT
    return root if root.endswith("/") else root + "/"


def is_gcs() -> bool:
    """True nếu data lake là GCS."""
    return lake_root().startswith("gs://")


def _root_prefix() -> str:
    """Phần sau scheme: ``""`` (bucket-per-layer) hoặc ``"my-lake/"``."""
    root = lake_root()
    for scheme in ("gs://", "s3a://", "s3://"):
        if root.startswith(scheme):
            return root[len(scheme):]
    return root


def path(layer: str, *parts: str) -> str:
    """Đường dẫn kiểu pyarrow (KHÔNG có scheme): ``my-lake/raw/transactions``.

    ``layer`` là tên tầng medallion. pyarrow nhận đường dẫn ``bucket/key...`` cho
    cả S3FileSystem lẫn GcsFileSystem nên một hàm dùng được cho hai backend.
    """
    joined = "/".join(p.strip("/") for p in (layer, *parts) if p)
    return f"{_root_prefix()}{joined}"


def uri(layer: str, *parts: str) -> str:
    """URI đầy đủ cho Spark: ``s3a://raw/transactions`` hoặc ``gs://b/raw/tra...``.

    Spark cần scheme để chọn connector (hadoop-aws vs gcs-connector), khác
    pyarrow vốn đã biết backend từ đối tượng filesystem.
    """
    joined = "/".join(p.strip("/") for p in (layer, *parts) if p)
    return f"{lake_root()}{joined}"


def filesystem() -> pafs.FileSystem:
    """pyarrow FileSystem đúng backend theo ``LAKE_ROOT``.

    GCS không có tham số ``allow_bucket_creation``: bucket phải tạo trước bằng
    Terraform/gcloud. Đó là khác biệt cố ý — ở local ``minio-init`` tạo bucket
    giúp, còn trên GCP việc tạo bucket thuộc về hạ tầng chứ không phải pipeline.
    """
    if is_gcs():
        return pafs.GcsFileSystem()
    return pafs.S3FileSystem(
        access_key=os.environ["MINIO_ROOT_USER"],
        secret_key=os.environ["MINIO_ROOT_PASSWORD"],
        endpoint_override=os.environ.get("MINIO_ENDPOINT", "minio:9000"),
        scheme="http",
        allow_bucket_creation=True,
    )
