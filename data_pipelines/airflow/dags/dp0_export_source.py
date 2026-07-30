"""DP0 — TEAM DATA: export `ops.*` (Postgres) -> MinIO `source`.

DAG này thuộc **team data**, không thuộc team ML. Nó là bước cuối của họ: đóng gói
dữ liệu vừa kết thúc ra file cho các team khác kéo về. Đây là ranh giới tổ chức duy
nhất trong luồng — từ đây trở đi team ML chỉ thấy file.

Giao dịch chảy vào Kafka; ingestion service của team data (``include.kafka_to_ops``,
chạy liên tục ngoài Airflow) ghi chúng xuống ``opsdb.ops.transactions``. Mỗi đầu
ngày DAG này dump trọn ngày vừa xong ra parquet.

Hai loại dữ liệu, hai nhịp:

* ``transactions`` — partition theo ngày, mỗi ngày một file mới
* 4 bảng reference — FULL SNAPSHOT, ghi đè bản hôm trước. Phải có snapshot mới mỗi
  ngày để DP2 dựng được SCD Type 2: không có gì để so thì ``is_current`` không bao
  giờ đổi và ba cột SCD2 tồn tại mà không chứng minh được gì.

Chạy **00:05**; team ML chạy tiếp lúc **00:15**. Nguồn là BẢNG chứ không phải Kafka
nên chạy lại ra kết quả y hệt và không phụ thuộc retention của Kafka.
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

# Chạy sau nửa đêm -> xử lý NGÀY HÔM TRƯỚC.
# Dùng ``logical_date - 1 ngày`` chứ không ``data_interval_start``: run thủ công và
# ``airflow tasks test`` gán interval = chính thời điểm chạy (không lùi một kỳ), nên
# data_interval_start sẽ trỏ sai ngày.
# Đổi về giờ HCM vì logical_date là UTC còn partition theo giờ địa phương.
DS_YESTERDAY = ('{{ (logical_date - macros.timedelta(days=1))'
                '.in_timezone("Asia/Ho_Chi_Minh").strftime("%Y-%m-%d") }}')

CODE = "cd /opt/airflow/code && MINIO_ENDPOINT=minio:9000"


@dag(
    dag_id="dp0_export_source",
    schedule="5 0 * * *",            # 00:05 — trước ml_pipeline (00:15)
    start_date=pendulum.datetime(2026, 7, 29, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    tags=["dp0", "team-data", "export"],
    doc_md=__doc__,
)
def dp0_export_source():
    # Reference data trước: DP2 cần snapshot dim để dựng SCD2, và nếu bước này lỗi
    # thì tốt hơn là lỗi TRƯỚC khi transactions được export.
    export_dims = BashOperator(
        task_id="export_dims",
        bash_command=f"{CODE} python -m include.ops_to_source --dims-only",
    )
    export_transactions = BashOperator(
        task_id="export_transactions",
        bash_command=(f"{CODE} python -m include.ops_to_source "
                      f"--no-dims --date {DS_YESTERDAY}"),
    )

    export_dims >> export_transactions


dp0_export_source()
