from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from feast import FeatureStore
from etl.feature_etl import build_features, check_source, validate_output

default_args = {
    "owner": "airflow",
    "retries": 1,
}

WINDOW = {"start": "{{ data_interval_start }}", "end": "{{ data_interval_end }}"}

def materialize_incremental(end):
    store = FeatureStore(repo_path="/opt/airflow/feature_store")
    store.materialize_incremental(end_date=datetime.fromisoformat(end))
    print(f"Materialized features up to {end}")


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
    t_materialize = PythonOperator(
        task_id="materialize_incremental",
        python_callable=materialize_incremental,
        op_kwargs={"end": "{{ data_interval_end }}"},
    )

    t_check >> t_build >> t_validate >> t_materialize
