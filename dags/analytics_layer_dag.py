import logging

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from helpers.get_ch_client import get_ch_client

logger = logging.getLogger(__name__)

def sales_performance_dtm():
    hook = PostgresHook(postgres_conn_id="warehouse_postgres_conn")

    rows = hook.get_records("""
        SELECT
            fs.purchase_date,
            dp.product_category,
            SUM(fs.purchase_amount) AS total_amount,
            AVG(fs.purchase_amount) AS mean_amount,
            COUNT(fs.transaction_id) AS transaction_count,
            COUNT(DISTINCT fs.customer_id) AS unique_customers
        FROM dds.fact_sales fs
        JOIN dds.dim_product dp
            ON fs.product_id = dp.product_id
        GROUP BY
            fs.purchase_date,
            dp.product_category
    """)

    conn = get_ch_client()
    ch_cursor = conn.cursor()

    ch_cursor.execute("""
        CREATE TABLE IF NOT EXISTS dm_datamart_1_sales_by_class_date
        (
            purchase_date Date,
            product_category String,
            total_amount Float64,
            mean_amount Float64,
            transaction_count UInt64,
            unique_customers UInt64
        )
        ENGINE = ReplacingMergeTree
        ORDER BY (purchase_date, product_category)
    """)
    
    ch_cursor.execute("TRUNCATE TABLE dm_datamart_1_sales_by_class_date")
    ch_cursor.executemany("""
        INSERT INTO dm_datamart_1_sales_by_class_date
        VALUES
    """, rows)
    ch_cursor.close()
    conn.close()


def customer_behavior_dtm():
    hook = PostgresHook(postgres_conn_id="warehouse_postgres_conn")
    conn = get_ch_client()
    ch_cursor = conn.cursor()


    ch_cursor.execute("""
        CREATE TABLE IF NOT EXISTS dm_datamart_2_customer_profile
        (
            customer_id String,
            total_amount Float64,
            total_items UInt64,
            preferred_category String,
            preferred_buying_day String
        )
        ENGINE = ReplacingMergeTree
        ORDER BY customer_id
    """)

    rows = hook.get_records("""
        WITH customer_stats AS (
            SELECT
                customer_id,
                SUM(purchase_amount) AS total_amount,
                COALESCE(SUM(quantity), 0)::INT8 AS total_items
            FROM dds.fact_sales
            GROUP BY customer_id
        ),

        favorite_category AS (
            SELECT DISTINCT ON (fs.customer_id)
                fs.customer_id,
                dp.product_category
            FROM dds.fact_sales fs
            JOIN dds.dim_product dp
                ON fs.product_id = dp.product_id
            GROUP BY
                fs.customer_id,
                dp.product_category
            ORDER BY
                fs.customer_id,
                COUNT(*) DESC
        ),

        favorite_day AS (
            SELECT DISTINCT ON (customer_id)
                customer_id,
                TO_CHAR(purchase_date, 'Day')
            FROM dds.fact_sales
            GROUP BY
                customer_id,
                TO_CHAR(purchase_date, 'Day')
            ORDER BY
                customer_id,
                COUNT(*) DESC
        )

        SELECT
            cs.customer_id,
            cs.total_amount,
            cs.total_items,
            fc.product_category,
            TRIM(fd.to_char)
        FROM customer_stats cs
        LEFT JOIN favorite_category fc
            ON cs.customer_id = fc.customer_id
        LEFT JOIN favorite_day fd
            ON cs.customer_id = fd.customer_id
    """)

    ch_cursor.execute("TRUNCATE TABLE dm_datamart_2_customer_profile")

    ch_cursor.executemany("""
        INSERT INTO dm_datamart_2_customer_profile
        VALUES
    """, rows)

    ch_cursor.close()
    conn.close()



def discount_effectiveness_dtm():
    hook = PostgresHook(postgres_conn_id="warehouse_postgres_conn")
    conn = get_ch_client()
    ch_cursor = conn.cursor()

    ch_cursor.execute("""
        CREATE TABLE IF NOT EXISTS dm_datamart_3_discount_analysis
        (
            discount_type String,
            total_quantity UInt64,
            total_amount Float64,
            transaction_count UInt64
        )
        ENGINE = ReplacingMergeTree
        ORDER BY discount_type
    """)

    rows = hook.get_records("""
        SELECT
            CASE
                WHEN discount_pct > 0 THEN 'DISCOUNT'
                ELSE 'NO_DISCOUNT'
            END AS discount_type,
            COALESCE(SUM(quantity), 0)::INT8 AS total_quantity,
            SUM(purchase_amount) AS total_amount,
            COUNT(transaction_id) AS transaction_count
        FROM dds.fact_sales
        GROUP BY 1
    """)

    ch_cursor.execute("TRUNCATE TABLE dm_datamart_3_discount_analysis")

    ch_cursor.executemany("""
        INSERT INTO dm_datamart_3_discount_analysis
        VALUES
    """, rows)

    ch_cursor.close()
    conn.close()


with DAG(
    dag_id="load_black_friday_datamarts",
    start_date=datetime(2026, 6, 4),
    schedule=None,
    catchup=False,
) as dag:
    START = EmptyOperator(task_id = "START")

    SALES_PERFORMANCE_DATAMART = PythonOperator(
        task_id="SALES_PERFORMANCE_DATAMART",
        python_callable=sales_performance_dtm,
    )

    CUSTOMER_BEHAVIOR_DATAMART = PythonOperator(
        task_id="CUSTOMER_BEHAVIOR_DATAMART",
        python_callable=customer_behavior_dtm,
    )

    DISCOUNT_EFFECTIVENESS_DATAMART = PythonOperator(
        task_id="DISCOUNT_EFFECTIVENESS_DATAMART",
        python_callable=discount_effectiveness_dtm,
    )

    END = EmptyOperator(task_id = "END")

    START >> SALES_PERFORMANCE_DATAMART >> CUSTOMER_BEHAVIOR_DATAMART >> DISCOUNT_EFFECTIVENESS_DATAMART >> END