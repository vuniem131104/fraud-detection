from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from etl.feature_etl import build_features, check_source, validate_output

default_args = {
    "owner": "airflow",
    "retries": 1,
}

# The daily [start, end) data interval is the incremental window: 00:00 the
# previous day -> 00:00 today. Features for transactions in this window are
# computed (using each active entity's full prior history) and upserted.
WINDOW = {"start": "{{ data_interval_start }}", "end": "{{ data_interval_end }}"}

with DAG(
    dag_id="feature_pipeline",
    start_date=datetime(2026, 7, 2),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["features", "etl"],
) as dag:

    t_check = PythonOperator(
        task_id="check_source",
        python_callable=check_source,
    )
    t_build = PythonOperator(
        task_id="build_features",
        python_callable=build_features,
        op_kwargs=WINDOW,
    )
    t_validate = PythonOperator(
        task_id="validate_output",
        python_callable=validate_output,
        op_kwargs=WINDOW,
    )

    t_check >> t_build >> t_validate
