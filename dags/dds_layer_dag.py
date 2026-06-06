import logging

import pandas as pd
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

def create_dds_layer_schema_tables():
    # Create dds schema and tables for dds data
    hook = PostgresHook(postgres_conn_id="warehouse_postgres_conn")
    hook.run(""" CREATE SCHEMA IF NOT EXISTS dds; """)
    hook.run("""
        -- =========================
        -- CUSTOMER DIMENSION
        -- =========================
        CREATE TABLE IF NOT EXISTS dds.dim_customer (
            customer_id TEXT PRIMARY KEY,
            age_group TEXT,
            gender TEXT,
            city TEXT,
            customer_segment TEXT
        );
        """)
    hook.run("""
        -- =========================
        -- PRODUCT DIMENSION
        -- =========================
        CREATE TABLE IF NOT EXISTS dds.dim_product (
            product_id TEXT PRIMARY KEY,
            product_category TEXT
        );
        """)
    hook.run("""
        -- =========================
        -- PAYMENT DIMENSION
        -- =========================
        CREATE TABLE IF NOT EXISTS dds.dim_payment_method (
            payment_method_id SERIAL PRIMARY KEY,
            payment_method TEXT UNIQUE
        );
        """)
    hook.run("""
        -- =========================
        -- SALES FACT TABLE
        -- =========================
        CREATE TABLE IF NOT EXISTS dds.fact_sales (
            transaction_id TEXT PRIMARY KEY,

            customer_id TEXT NOT NULL,
            product_id TEXT NOT NULL,
            payment_method_id INTEGER NOT NULL,

            purchase_date TIMESTAMPTZ,
            purchase_hour INTEGER,

            original_price NUMERIC,
            discount_pct NUMERIC,
            final_price NUMERIC,
            quantity NUMERIC,
            purchase_amount NUMERIC,

            is_weekend NUMERIC,
            is_black_friday NUMERIC,

            CONSTRAINT fk_customer
                FOREIGN KEY (customer_id)
                REFERENCES dds.dim_customer(customer_id),

            CONSTRAINT fk_product
                FOREIGN KEY (product_id)
                REFERENCES dds.dim_product(product_id),

            CONSTRAINT fk_payment
                FOREIGN KEY (payment_method_id)
                REFERENCES dds.dim_payment_method(payment_method_id)
        );
        """)

def load_dds_layer():
    # Create dds schema and tables for dds data
    hook = PostgresHook(postgres_conn_id="warehouse_postgres_conn")
    hook.run("""
        -- =========================
        -- LOAD CUSTOMER DIMENSION
        -- =========================
        INSERT INTO dds.dim_customer (
            customer_id,
            age_group,
            gender,
            city,
            customer_segment
        )
        SELECT DISTINCT
            customer_id,
            age_group,
            gender,
            city,
            customer_segment
        FROM raw.black_friday_sales
        WHERE customer_id IS NOT NULL
        ON CONFLICT (customer_id) DO NOTHING;
        """)
    hook.run("""
        -- =========================
        -- LOAD PRODUCT DIMENSION
        -- =========================
        INSERT INTO dds.dim_product (
            product_id,
            product_category
        )
        SELECT DISTINCT
            product_id,
            product_category
        FROM raw.black_friday_sales
        WHERE product_id IS NOT NULL
        ON CONFLICT (product_id) DO NOTHING;
        """)
    hook.run("""
        -- =========================
        -- LOAD PAYMENT DIMENSION
        -- =========================
        INSERT INTO dds.dim_payment_method (
            payment_method
        )
        SELECT DISTINCT
            payment_method
        FROM raw.black_friday_sales
        WHERE payment_method IS NOT NULL
        ON CONFLICT (payment_method) DO NOTHING;
        """)
    hook.run("""
        -- =========================
        -- LOAD FACT TABLE
        -- =========================
        INSERT INTO dds.fact_sales (
            transaction_id,
            customer_id,
            product_id,
            payment_method_id,
            purchase_date,
            purchase_hour,
            original_price,
            discount_pct,
            final_price,
            quantity,
            purchase_amount,
            is_weekend,
            is_black_friday
        )
        SELECT
            r.transaction_id,
            r.customer_id,
            r.product_id,
            pm.payment_method_id,
            r.purchase_date,
            r.purchase_hour,
            r.original_price,
            r.discount_pct,
            r.final_price,
            r.quantity,
            r.purchase_amount,
            r.is_weekend,
            r.is_black_friday
        FROM raw.black_friday_sales r
        JOIN dds.dim_payment_method pm
            ON pm.payment_method = r.payment_method
        ON CONFLICT (transaction_id) DO NOTHING;
        """)

with DAG(
    dag_id = 'dds_layer',
    description='Created dds layer and fill it',
    schedule_interval=None,
    start_date=datetime(2026, 6, 2),
    catchup=False,
    tags=['data_upload'],
) as dag:
    START = EmptyOperator(task_id = "START")

    CREATE_DDS_LAYER = PythonOperator(
        task_id = "CREATE_DDS_LAYER",
        python_callable = create_dds_layer_schema_tables
    )

    LOAD_DDS_LAYER = PythonOperator(
        task_id = "LOAD_DDS_LAYER",
        python_callable = load_dds_layer
    )

    END = EmptyOperator(task_id = "END")

    START >> CREATE_DDS_LAYER >> LOAD_DDS_LAYER >> END