from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from scripts.raw_layer.raw_layer import rl_load_data_from_mongo, rl_load_data_from_minio, rl_reject_inconsistency, rl_delete_inconsistency

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

    REJECT_INCONSISTEN_DATA = PythonOperator(
        task_id = "REJECT_INCONSISTEN_DATA",
        python_callable = rl_reject_inconsistency
    )

    DELETE_INCONSISTEN_DATA = PythonOperator(
        task_id = "DELETE_INCONSISTEN_DATA",
        python_callable = rl_delete_inconsistency
    )

    UPLOAD_INTO_DDS = TriggerDagRunOperator(
        task_id = "UPLOAD_INTO_DDS",
        trigger_dag_id = "dds_layer",
        wait_for_completion = False
    )

    END = EmptyOperator(task_id = "END")

    START >> LOAD_DATA_POSTGRES_MINIO >> LOAD_DATA_POSTGRES_MONGO >> REJECT_INCONSISTEN_DATA >> DELETE_INCONSISTEN_DATA >> UPLOAD_INTO_DDS >> END