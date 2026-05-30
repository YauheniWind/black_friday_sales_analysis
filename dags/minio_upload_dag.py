import os
import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from helpers.get_minio_client import get_minio_client

logger = logging.getLogger(__name__)
local_dir = "/opt/airflow/data/source_s3"
bucket_name = "sales-data"

def create_minio_buckets():
    s3_client = get_minio_client()
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except:
        s3_client.create_bucket(Bucket=bucket_name)

def upload_file_to_s3():
    s3_client = get_minio_client()

    for file_name in os.listdir(local_dir):
        file_path = os.path.join(local_dir, file_name)
        logger.info(f"""
                        File {file_path} in bucket {bucket_name}
                    """)
        if os.path.isfile(file_path):
            s3_client.upload_file(
                Filename=file_path,
                Bucket=bucket_name,
                Key=file_name
            )

    print("All files uploaded")

with DAG(
    dag_id = 'upload_to_s3',
    description='Upload the data to s3 bucket',
    schedule_interval=None,
    start_date=datetime(2026, 5, 30),
    catchup=False,
    tags=['data_upload'],
) as dag:
    START = EmptyOperator(task_id = "START")

    CREATE_MINIO_BUCKET = PythonOperator(
        task_id = "CREATE_MINIO_BUCKET",
        python_callable = create_minio_buckets
    )

    UPLOAD_DATA_MINIO = PythonOperator(
        task_id = "UPLOAD_DATA_MINIO",
        python_callable = upload_file_to_s3
    )

    END = EmptyOperator(task_id = "END")

    START >> CREATE_MINIO_BUCKET >> UPLOAD_DATA_MINIO >> END