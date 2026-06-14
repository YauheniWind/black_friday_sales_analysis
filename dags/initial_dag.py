from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from scripts.initial.initial_infra import initial_bucket, initial_raw, initial_dds, initial_datamarts 

with DAG(
    dag_id = 'Initial_DAG',
    description='Create the initial infrastructure',
    schedule_interval=None,
    start_date=datetime(2026, 6, 2),
    catchup=False,
    tags=['create_infrastructure'],
) as dag:
    START = EmptyOperator(task_id = "START")

    CREATE_BUCKET = PythonOperator(
        task_id = "CREATE_BUCKET",
        python_callable = initial_bucket
    )

    CREATE_RAW_LAYER = PythonOperator(
        task_id = "CREATE_RAW_LAYER",
        python_callable = initial_raw
    )

    CREATE_DDS_LAYER = PythonOperator(
        task_id = "CREATE_DDS_LAYER",
        python_callable = initial_dds
    )

    CREATE_DATAMART_LAYER = PythonOperator(
        task_id = "CREATE_DATAMART_LAYER",
        python_callable = initial_datamarts
    )

    END = EmptyOperator(task_id = "END")

    START >> CREATE_BUCKET >> CREATE_RAW_LAYER >> CREATE_DDS_LAYER >> CREATE_DATAMART_LAYER  >> END