from datetime import datetime

from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from scripts.upload_data.upload_data import upload_mongo

with DAG(
    dag_id = 'upload_to_mongo',
    description='Upload the data to MongoDB',
    schedule_interval=None,
    start_date=datetime(2026, 5, 30),
    catchup=False,
    tags=['data_upload'],
) as dag:
    START = EmptyOperator(task_id = "START")

    UPLOAD_DATA_MONGO = PythonOperator(
        task_id = "UPLOAD_DATA_MONGO",
        python_callable = upload_mongo
    )

    END = EmptyOperator(task_id = "END")

    START >> UPLOAD_DATA_MONGO >> END