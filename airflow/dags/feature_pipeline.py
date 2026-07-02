from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

from etl.feature_etl import build_transaction_features

default_args = {
    "owner": "airflow",
}

with DAG(
    dag_id="feature_pipeline",
    start_date=datetime(2026, 7, 2),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
) as dag:

    feature_task = PythonOperator(
        task_id="build_transaction_features",
        python_callable=build_transaction_features,
    )