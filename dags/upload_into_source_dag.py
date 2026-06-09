import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from scripts.upload_data.upload_data import upload_file_to_s3, upload_mongo

logger = logging.getLogger(__name__)

with DAG(
    dag_id = 'upload_into_sources',
    description='Upload the data to s3 bucket and mongo db',
    schedule_interval=None,
    start_date=datetime(2026, 5, 30),
    catchup=False,
    tags=['data_upload'],
) as dag:
    START = EmptyOperator(task_id = "START")

    UPLOAD_DATA_MINIO = PythonOperator(
        task_id = "UPLOAD_DATA_MINIO",
        python_callable = upload_file_to_s3
    )

    UPLOAD_DATA_MONGO = PythonOperator(
        task_id = "UPLOAD_DATA_MONGO",
        python_callable = upload_mongo
    )

    END = EmptyOperator(task_id = "END")

    START >> UPLOAD_DATA_MINIO >> UPLOAD_DATA_MONGO >> END