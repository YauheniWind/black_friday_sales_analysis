import logging

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from scripts.dds_layer.dds_layer import dl_load_into_dds

logger = logging.getLogger(__name__)

with DAG(
    dag_id = 'dds_layer',
    description='Created dds layer and fill it',
    schedule_interval=None,
    start_date=datetime(2026, 6, 2),
    catchup=False,
    tags=['data_upload'],
) as dag:
    START = EmptyOperator(task_id = "START")

    LOAD_DDS_LAYER = PythonOperator(
        task_id = "LOAD_DDS_LAYER",
        python_callable = dl_load_into_dds
    )

    END = EmptyOperator(task_id = "END")

    START >> LOAD_DDS_LAYER >> END