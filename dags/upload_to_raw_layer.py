from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from scripts.raw_layer.raw_layer import rl_load_data_from_mongo, rl_load_data_from_minio

with DAG(
    dag_id = 'upload_to_postgres',
    description='Upload the data to posgtres',
    schedule_interval=None,
    start_date=datetime(2026, 6, 2),
    catchup=False,
    tags=['data_upload'],
) as dag:
    START = EmptyOperator(task_id = "START")

    LOAD_DATA_POSTGRES_MINIO = PythonOperator(
        task_id = "LOAD_DATA_POSTGRES_MINIO",
        python_callable = rl_load_data_from_minio
    )

    LOAD_DATA_POSTGRES_MONGO = PythonOperator(
        task_id = "LOAD_DATA_POSTGRES_MONGO",
        python_callable = rl_load_data_from_mongo
    )

    END = EmptyOperator(task_id = "END")

    START >> LOAD_DATA_POSTGRES_MINIO >> LOAD_DATA_POSTGRES_MONGO  >> END