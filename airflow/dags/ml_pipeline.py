"""TEAM ML — ba pipeline chạy NỐI TIẾP: DP1 -> DP2 -> DP3.

Mỗi ngày 00:15 (sau khi team data export xong lúc 00:05):

    DP1  ingest    source -> raw (Bronze)        giữ data THÔ, không sửa gì
         ├─ transactions: chỉ partition của ngày vừa xong (incremental)
         └─ reference   : full snapshot, ghi đè (cần cho SCD2 ở DP2)

    DP2  transform raw -> staging -> curated     Spark
         ├─ bronze_to_silver : mergeSchema + dedup theo id
         ├─ gold_fact        : fact_transactions, partition event_date
         └─ gold_dims        : SCD Type 2 cho 4 dim

    DP3  features  curated -> Postgres -> Redis
         ├─ serving_features  : 4 bảng feat_* (snapshot as-of hôm nay)
         ├─ training_features : feat_training (point-in-time từng giao dịch)
         ├─ validate          : bảng không được rỗng
         └─ materialize       : CHỈ các view batch lên Redis

Vì sao một DAG với ba TaskGroup thay vì ba DAG
---------------------------------------------
Yêu cầu là "DP1 xong thì tới DP2, DP2 xong thì tới DP3". Ba DAG riêng phải nối bằng
lịch giờ (mong DP1 xong trước 00:30) hoặc bằng sensor — cả hai đều gãy khi một bước
chạy lâu hơn dự kiến, và gãy KHÔNG BÁO: DP2 chạy trên Bronze thiếu partition rồi
báo thành công. Một DAG thì Airflow bảo đảm thứ tự và một task fail sẽ chặn phần
sau. Trên UI vẫn thấy rõ ba nhóm.

Airflow không tự chạy Spark mà submit **batch Dataproc Serverless**: không có cụm
Spark nào trên VM, mỗi job là một batch riêng, tính tiền theo thời gian chạy.
"""

from __future__ import annotations

import os
import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Variable, dag, get_current_context, task, task_group

from include.lake_io import copy_dataset, copy_partition, get_lake_fs

# Danh sách view được materialize lấy từ module dùng chung — KHÔNG hardcode ở đây.
# Thêm một FeatureView mới mà quên sửa DAG sẽ làm nó âm thầm không lên Redis.
for _c in (os.environ.get("SHARED_DIR"), "/opt/airflow/shared",
           str(Path(__file__).resolve().parents[3] / "shared")):
    if _c and Path(_c).is_dir():
        sys.path.insert(0, _c)
        break
import feature_windows as W  # noqa: E402

DIMS = ["users", "cards", "merchants", "devices"]

# Ngày xử lý = NGÀY HÔM TRƯỚC theo giờ HCM. Dùng logical_date-1d chứ không dùng
# data_interval_start (run thủ công gán interval = chính thời điểm chạy).
DS_HCM = ('{{ (logical_date - macros.timedelta(days=1))'
          '.in_timezone("Asia/Ho_Chi_Minh").strftime("%Y-%m-%d") }}')

# --- Dataproc Serverless ---------------------------------------------------
# Job Spark KHÔNG chạy trên VM. File .py phải nằm trên GCS (Dataproc không thấy
# volume của VM), deploy bằng:
#   gcloud storage rsync -r spark/jobs $DATAPROC_CODE_ROOT
DATAPROC_CODE_ROOT = os.environ.get("DATAPROC_CODE_ROOT", "")
DATAPROC_REGION = os.environ.get("DATAPROC_REGION", os.environ.get("GCP_REGION", ""))


def _dataproc_batch(script: str, args: list[str]) -> dict:
    """Cấu hình một PySpark batch trên Dataproc Serverless.

    Hai thứ BẮT BUỘC ship kèm, thiếu là job chết ngay:
      * ``python_file_uris``  -> shared/feature_windows.py. dp3_* import module này
        qua SHARED_DIR, thứ chỉ tồn tại vì docker-compose mount ./shared. Trên
        Dataproc không có mount nào.
      * ``jar_file_uris``     -> driver JDBC Postgres. Dataproc Serverless không
        chắc ra được Maven nên đưa jar lên GCS.

    Env của job truyền bằng ``spark.dataproc.driverEnv.*``: các job đọc os.environ
    (LAKE_ROOT, PG_*), mà Dataproc Serverless không có cách nào khác để set env.
    """
    props = {
        f"spark.dataproc.driverEnv.{k}": v
        for k, v in {
            "LAKE_ROOT": os.environ.get("LAKE_ROOT", ""),
            "PG_HOST": os.environ.get("PG_HOST", ""),
            "PG_DB": os.environ.get("WAREHOUSE_POSTGRES_DB", "warehouse"),
            "PG_USER": os.environ.get("AIRFLOW_USER", ""),
            # CẢNH BÁO: property của batch đọc được bằng `gcloud dataproc batches
            # describe`. Môi trường thật nên chuyển sang Secret Manager.
            "PG_PASSWORD": os.environ.get("AIRFLOW_PASSWORD", ""),
        }.items() if v
    }
    exec_cfg = {
        k: v for k, v in {
            "service_account": os.environ.get("DATAPROC_SERVICE_ACCOUNT", ""),
            "subnetwork_uri": os.environ.get("DATAPROC_SUBNET", ""),
        }.items() if v
    }
    batch: dict = {
        "pyspark_batch": {
            "main_python_file_uri": f"{DATAPROC_CODE_ROOT.rstrip('/')}/{script}",
            "args": args,
        },
        "runtime_config": {"properties": props},
    }
    if pyfiles := os.environ.get("DATAPROC_PYFILES", ""):
        batch["pyspark_batch"]["python_file_uris"] = pyfiles.split(",")
    if jars := os.environ.get("DATAPROC_JDBC_JAR", ""):
        batch["pyspark_batch"]["jar_file_uris"] = jars.split(",")
    if exec_cfg:
        batch["environment_config"] = {"execution_config": exec_cfg}
    return batch


def spark_task(task_id: str, script: str, *args: str) -> DataprocCreateBatchOperator:
    """Một task Spark = một batch Dataproc Serverless."""
    return DataprocCreateBatchOperator(
        task_id=task_id,
        project_id=os.environ.get("GCP_PROJECT_ID", ""),
        region=DATAPROC_REGION,
        # batch_id chỉ nhận [a-z0-9-]: task_id có dấu _ nên phải đổi.
        # try_number PHẢI đi qua `ti`: context của Airflow 3 không có key phẳng
        # try_number, dùng "{{ try_number }}" là UndefinedError lúc render và task
        # chết trước cả khi submit batch. Nó có mặt ở đây để lần retry tạo batch_id
        # khác — Dataproc từ chối batch_id trùng bằng ALREADY_EXISTS.
        batch_id=(f"{task_id.replace('_', '-')}-"
                  "{{ ts_nodash | lower }}-{{ ti.try_number }}"),
        batch=_dataproc_batch(script, list(args)),
    )


# Mọi bảng feature phải có > 0 dòng. Đủ cho MVP, và quan trọng là nó CHẶN
# materialize: đẩy một bảng rỗng lên Redis sẽ xoá sạch feature đang phục vụ.
FEATURE_TABLES = ["feat_card", "feat_user", "feat_merchant", "feat_device",
                  "feat_training"]
VALIDATE = (
    f'for t in {" ".join(FEATURE_TABLES)}; do '
    'N=$(PGPASSWORD="$AIRFLOW_PASSWORD" psql -h "$PG_HOST" -U "$AIRFLOW_USER" '
    '-d "$WAREHOUSE_POSTGRES_DB" -tAc "SELECT count(*) FROM application.$t"); '
    'echo "$t=$N"; test "$N" -gt 0 || { echo "FAIL: $t rỗng"; exit 1; }; done'
)

# feast materialize: KHÔNG dùng materialize-incremental. DP3 ghi lại feat_* với
# event_timestamp = 00:00 của ngày xử lý, không "mới hơn" mốc incremental của lần
# chạy trước -> bản cập nhật bị bỏ qua và Redis giữ giá trị cũ. Dùng khoảng tường
# minh phủ 2 ngày gần nhất để luôn ghi đè.
MATERIALIZE = (
    'cd /opt/airflow/feature_store && feast materialize '
    '{{ (logical_date - macros.timedelta(days=2)).strftime("%Y-%m-%dT00:00:00") }} '
    '"$(date -u +%Y-%m-%dT%H:%M:%S)" '
    + " ".join(f"--views {v}" for v in W.BATCH_VIEWS)
)


def _processing_date() -> str:
    """Ngày xử lý (giờ HCM) cho các task Python."""
    ld = get_current_context()["logical_date"] - timedelta(days=1)
    return ld.in_timezone("Asia/Ho_Chi_Minh").strftime("%Y-%m-%d")


@dag(
    dag_id="ml_pipeline",
    schedule="15 0 * * *",           # 00:15 — sau DP0 (00:05)
    start_date=pendulum.datetime(2026, 7, 29, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    max_active_runs=1,               # hai run cùng lúc sẽ tranh nhau Bronze/Silver
    tags=["ml", "medallion", "feature-store"],
    doc_md=__doc__,
)
def ml_pipeline():

    # ------------------------------------------------------------------- DP1
    @task_group(group_id="dp1_ingest_bronze")
    def dp1():
        """source -> raw (Bronze). Bronze giữ data THÔ: không dedup, không sửa kiểu.

        Duplicate và schema khác nhau giữa các partition được giữ nguyên ở đây; đó
        là bằng chứng để DP2 chứng minh nó xử lý được.
        """

        @task
        def ingest_transactions() -> str:
            """Copy đúng partition của ngày xử lý (incremental)."""
            ds = _processing_date()
            fs = get_lake_fs()
            n = copy_partition(fs, Variable.get("source_bucket", default="source"),
                               Variable.get("bronze_bucket", default="raw"),
                               "transactions", ds)
            return f"transactions event_date={ds}: {n} file"

        @task
        def ingest_reference() -> str:
            """Copy FULL SNAPSHOT 4 bảng reference (ghi đè bản hôm trước).

            Phải copy mỗi ngày: DP2 dựng SCD2 bằng cách so snapshot này với bản
            ``is_current`` ở Gold. Không refresh thì không có thay đổi nào bị bắt.
            """
            fs = get_lake_fs()
            src = Variable.get("source_bucket", default="source")
            dst = Variable.get("bronze_bucket", default="raw")
            out = {d: copy_dataset(fs, src, dst, d) for d in DIMS}
            return "  ".join(f"{k}={v}" for k, v in out.items())

        ingest_transactions() >> ingest_reference()

    # ------------------------------------------------------------------- DP2
    @task_group(group_id="dp2_transform")
    def dp2():
        """raw -> staging (Silver) -> curated (Gold) bằng Spark."""
        bronze_to_silver = spark_task(
            "bronze_to_silver", "dp2_bronze_to_silver.py", "--date", DS_HCM)
        gold_fact = spark_task(
            "gold_fact", "dp2_silver_to_gold.py", "--stage", "fact", "--date", DS_HCM)
        # SCD2 đọc snapshot dim ở Bronze, không phụ thuộc Silver -> chạy song song
        # với gold_fact được. Nhưng DP3 cần CẢ HAI xong.
        gold_dims = spark_task(
            "gold_dims", "dp2_silver_to_gold.py", "--stage", "dims")
        bronze_to_silver >> [gold_fact, gold_dims]

    # ------------------------------------------------------------------- DP3
    @task_group(group_id="dp3_features")
    def dp3():
        """curated -> Postgres (offline store) -> Redis (online store)."""
        serving_features = spark_task(
            "serving_features", "dp3_gold_to_features.py", "--date", DS_HCM)
        # Bảng offline cho training: point-in-time từng giao dịch, gồm CẢ ba tầng
        # feature (batch + Flink 10'/1h + velocity 5'). Bảng này không lên Redis.
        training_features = spark_task(
            "training_features", "dp3_training_features.py")
        validate = BashOperator(task_id="validate", bash_command=VALIDATE)
        # CHỈ view batch. Materialize view của Flink sẽ ghi giá trị batch (chậm tới
        # 24h) lên giá trị real-time -> serving đọc số của đêm qua.
        materialize = BashOperator(task_id="materialize", bash_command=MATERIALIZE)

        [serving_features, training_features] >> validate >> materialize

    dp1() >> dp2() >> dp3()


ml_pipeline()
