from airflow.providers.postgres.hooks.postgres import PostgresHook

from scripts.helpers.get_minio_client import get_minio_client
from scripts.helpers.get_ch_client import get_ch_client

def initail_bucket():
    bucket_name = "sales-data"
    s3_client = get_minio_client()
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except:
        s3_client.create_bucket(Bucket=bucket_name)

def initail_raw():
    # Create raw schema and table for raw data
    hook = PostgresHook(postgres_conn_id="warehouse_postgres_conn")
    hook.run(""" CREATE SCHEMA IF NOT EXISTS raw; """)
    hook.run("""
        CREATE TABLE IF NOT EXISTS raw.black_friday_sales (
            transaction_id TEXT PRIMARY KEY,
            customer_id TEXT,
            age_group TEXT,
            gender TEXT,
            city TEXT,
            customer_segment TEXT,
            product_id TEXT,
            product_category TEXT,
            original_price NUMERIC,
            discount_pct NUMERIC,
            final_price NUMERIC,
            quantity NUMERIC,
            purchase_amount NUMERIC,
            payment_method TEXT,
            purchase_date TIMESTAMPTZ,
            purchase_hour INTEGER,
            is_weekend NUMERIC,
            is_black_friday NUMERIC
            
        );
    """)
    hook.run("""
        CREATE TABLE IF NOT EXISTS raw.black_friday_sales_rejected(
            LIKE raw.black_friday_sales INCLUDING ALL,
            rejection_reason TEXT,
            rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

def initail_dds():
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


def initial_datamarts():
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
    ch_cursor.close()
    conn.close()
