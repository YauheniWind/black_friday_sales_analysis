import logging
import pandas as pd
from datetime import datetime
from pymongo import MongoClient


from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


from helpers.get_minio_client import get_minio_client

logger = logging.getLogger(__name__)

def create_raw_layer_schema_table():
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

def load_data_postgres_mongo():
    cfg = Variable.get("lesson_38_mongo_uri")
    with MongoClient(cfg) as client:
        coll = client["source_db"]["black_friday_sales"]
        # Get whole collection
        rows = list(coll.find())
        mongo_ids = [row["_id"] for row in rows] # Save all IDs
        norm_rows = []
        # Normalize values
        for row in rows:
            norm_rows.append(
                [
                    str(row["transaction_id"]),
                    str(row["customer_id"]),
                    str(row["age_group"]),
                    str(row["gender"]),
                    str(row["city"]),
                    str(row["customer_segment"]),
                    str(row["product_id"]),
                    str(row["product_category"]),
                    float(row["original_price"]),
                    int(row["discount_pct"]),
                    float(row["final_price"]),
                    int(row["quantity"]),
                    float(row["purchase_amount"]),
                    str(row["payment_method"]),
                    str(row["purchase_date"]),
                    int(row["purchase_hour"]),
                    int(row["is_weekend"]),
                    int(row["is_black_friday"]),
                ]
            )
        logger.info(f"Loaded {len(norm_rows)} rows")
        hook = PostgresHook(postgres_conn_id="warehouse_postgres_conn")
        sql = f"""
                INSERT INTO raw.black_friday_sales (transaction_id, customer_id, age_group, gender, city, customer_segment,
                                                    product_id, product_category, original_price, discount_pct, final_price,
                                                    quantity, purchase_amount, payment_method, purchase_date, purchase_hour,
                                                    is_weekend, is_black_friday)
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                on conflict (transaction_id)
                do update set
                    age_group = EXCLUDED.age_group,
                    gender = EXCLUDED.gender,
                    city = EXCLUDED.city,
                    customer_segment = EXCLUDED.customer_segment,
                    product_id = EXCLUDED.product_id,
                    product_category = EXCLUDED.product_category,
                    original_price = EXCLUDED.original_price,
                    discount_pct = EXCLUDED.discount_pct,
                    final_price = EXCLUDED.final_price,
                    quantity = EXCLUDED.quantity,
                    purchase_amount = EXCLUDED.purchase_amount,
                    payment_method = EXCLUDED.payment_method,
                    purchase_date = EXCLUDED.purchase_date,
                    purchase_hour = EXCLUDED.purchase_hour,
                    is_weekend = EXCLUDED.is_weekend,
                    is_black_friday = EXCLUDED.is_black_friday;
                """
        conn = hook.get_conn()
        with conn.cursor() as cursor:
            cursor.executemany(sql, norm_rows)
        conn.commit()
        coll.delete_many({"_id": {"$in": mongo_ids}}) # Drop all loaded IDs
        conn.close()


def load_data_postgres_minio():
    s3_client = get_minio_client()
    hook = PostgresHook(postgres_conn_id="warehouse_postgres_conn")
    conn = hook.get_conn()
    response = s3_client.list_objects_v2(Bucket='sales-data')

    for obj in response.get('Contents', []):
        key = obj["Key"]

        # Process only csv files
        if not key.endswith(".csv"):
            continue

        print(f"Processing {key}")
        
        # Download file
        file_obj = s3_client.get_object(
            Bucket="sales-data",
            Key=key
        )

        # Read CSV
        df = pd.read_csv(file_obj["Body"])

        if df.empty:
            print(f"Skipping empty file: {key}")
            continue

        # Convert dataframe to list of tuples
        rows = [tuple(row) for row in df.itertuples(index=False, name=None)]

        sql = f"""
                INSERT INTO raw.black_friday_sales (transaction_id, customer_id, age_group, gender, city, customer_segment,
                                                    product_id, product_category, original_price, discount_pct, final_price,
                                                    quantity, purchase_amount, payment_method, purchase_date, purchase_hour,
                                                    is_weekend, is_black_friday)
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                on conflict (transaction_id)
                do update set
                    age_group = EXCLUDED.age_group,
                    gender = EXCLUDED.gender,
                    city = EXCLUDED.city,
                    customer_segment = EXCLUDED.customer_segment,
                    product_id = EXCLUDED.product_id,
                    product_category = EXCLUDED.product_category,
                    original_price = EXCLUDED.original_price,
                    discount_pct = EXCLUDED.discount_pct,
                    final_price = EXCLUDED.final_price,
                    quantity = EXCLUDED.quantity,
                    purchase_amount = EXCLUDED.purchase_amount,
                    payment_method = EXCLUDED.payment_method,
                    purchase_date = EXCLUDED.purchase_date,
                    purchase_hour = EXCLUDED.purchase_hour,
                    is_weekend = EXCLUDED.is_weekend,
                    is_black_friday = EXCLUDED.is_black_friday;
                """
        with conn.cursor() as cursor:
            cursor.executemany(sql, rows)
        conn.commit()
        conn.close()

        print(f"Inserted {len(rows)} rows from {key}")

        # Delete file after successful load
        s3_client.delete_object(
            Bucket="sales-data",
            Key=key
        )

        print(f"Deleted {key}")
    print("Load completed")

print("Done")

with DAG(
    dag_id = 'upload_to_postgres',
    description='Upload the data to posgtres',
    schedule_interval=None,
    start_date=datetime(2026, 6, 2),
    catchup=False,
    tags=['data_upload'],
) as dag:
    START = EmptyOperator(task_id = "START")

    CREATE_RAW_BASE = PythonOperator(
        task_id = "CREATE_RAW_BASE",
        python_callable = create_raw_layer_schema_table
    )

    LOAD_DATA_POSTGRES_MINIO = PythonOperator(
        task_id = "LOAD_DATA_POSTGRES_MINIO",
        python_callable = load_data_postgres_minio
    )

    LOAD_DATA_POSTGRES_MONGO = PythonOperator(
        task_id = "LOAD_DATA_POSTGRES_MONGO",
        python_callable = load_data_postgres_mongo
    )

    END = EmptyOperator(task_id = "END")

    START >> CREATE_RAW_BASE >> LOAD_DATA_POSTGRES_MINIO >> LOAD_DATA_POSTGRES_MONGO  >> END