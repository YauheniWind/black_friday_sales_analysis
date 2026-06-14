from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from scripts.core_layer.core_layer import cl_sales_performance_dtm, cl_customer_behavior_dtm, cl_discount_effectiveness_dtm

with DAG(
    dag_id="load_black_friday_datamarts",
    start_date=datetime(2026, 6, 4),
    schedule=None,
    catchup=False,
) as dag:
    START = EmptyOperator(task_id = "START")

    SALES_PERFORMANCE_DATAMART = PythonOperator(
        task_id="SALES_PERFORMANCE_DATAMART",
        python_callable=cl_sales_performance_dtm,
    )

    CUSTOMER_BEHAVIOR_DATAMART = PythonOperator(
        task_id="CUSTOMER_BEHAVIOR_DATAMART",
        python_callable=cl_customer_behavior_dtm,
    )

    DISCOUNT_EFFECTIVENESS_DATAMART = PythonOperator(
        task_id="DISCOUNT_EFFECTIVENESS_DATAMART",
        python_callable=cl_discount_effectiveness_dtm,
    )

    END = EmptyOperator(task_id = "END")

    START >> SALES_PERFORMANCE_DATAMART >> CUSTOMER_BEHAVIOR_DATAMART >> DISCOUNT_EFFECTIVENESS_DATAMART >> END