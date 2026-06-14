import logging
import pandas as pd
from pymongo import MongoClient

from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook


from scripts.helpers.get_minio_client import get_minio_client

logger = logging.getLogger(__name__)

def rl_load_data_from_mongo():
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


def rl_load_data_from_minio():
    s3_client = get_minio_client()
    hook = PostgresHook(postgres_conn_id="warehouse_postgres_conn")
    conn = hook.get_conn()
    response = s3_client.list_objects_v2(Bucket='sales-data')

    for obj in response.get('Contents', []):
        key = obj["Key"]

        # Process only csv files
        if not key.endswith(".csv"):
            continue

        logger.info(f"Processing {key}")

        # Download file
        file_obj = s3_client.get_object(
            Bucket="sales-data",
            Key=key
        )

        # Read CSV
        df = pd.read_csv(file_obj["Body"])

        if df.empty:
            logger.info(f"Skipping empty file: {key}")
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

        logger.info(f"Inserted {len(rows)} rows from {key}")

        # Delete file after successful load
        s3_client.delete_object(
            Bucket="sales-data",
            Key=key
        )

        logger.info(f"Deleted {key}")
    logger.info("Load completed")

logger.info("Done")

def rl_reject_inconsistency():
    hook = PostgresHook(postgres_conn_id="warehouse_postgres_conn")
    conn = hook.get_conn()

    sql = """
            INSERT INTO raw.black_friday_sales_rejected
            (
                transaction_id,
                customer_id,
                age_group,
                gender,
                city,
                customer_segment,
                product_id,
                product_category,
                original_price,
                discount_pct,
                final_price,
                quantity,
                purchase_amount,
                payment_method,
                purchase_date,
                purchase_hour,
                is_weekend,
                is_black_friday,
                rejection_reason
            )
            SELECT
                transaction_id,
                customer_id,
                age_group,
                gender,
                city,
                customer_segment,
                product_id,
                product_category,
                original_price,
                discount_pct,
                final_price,
                quantity,
                purchase_amount,
                payment_method,
                purchase_date,
                purchase_hour,
                is_weekend,
                is_black_friday,

                CASE
                    WHEN purchase_hour NOT BETWEEN 0 AND 23
                        THEN 'Invalid purchase hour'

                    WHEN discount_pct NOT BETWEEN 0 AND 100
                        THEN 'Invalid discount percentage'

                    WHEN quantity <= 0
                        THEN 'Invalid quantity'

                    WHEN original_price <= 0
                        THEN 'Invalid original price'

                    WHEN final_price <= 0
                        THEN 'Invalid final price'

                    WHEN ABS(purchase_amount - final_price * quantity) > 0.01
                        THEN 'Purchase amount mismatch'

                    WHEN is_weekend NOT IN (0,1)
                        THEN 'Invalid weekend flag'

                    WHEN is_black_friday NOT IN (0,1)
                        THEN 'Invalid black friday flag'

                    ELSE 'Unknown validation error'
                END AS rejection_reason

            FROM raw.black_friday_sales
            WHERE
                purchase_hour NOT BETWEEN 0 AND 23
                OR discount_pct NOT BETWEEN 0 AND 100
                OR quantity <= 0
                OR original_price <= 0
                OR final_price <= 0
                OR ABS(purchase_amount - final_price * quantity) > 0.01
                OR is_weekend NOT IN (0,1)
                OR is_black_friday NOT IN (0,1);
                """
    with conn.cursor() as cursor:
        cursor.execute(sql)
    conn.commit()
    conn.close()

def rl_delete_inconsistency():
    hook = PostgresHook(postgres_conn_id="warehouse_postgres_conn")
    conn = hook.get_conn()
    sql = """
            DELETE FROM raw.black_friday_sales b
            USING raw.black_friday_sales_rejected r
            WHERE b.transaction_id = r.transaction_id;
        """
    with conn.cursor() as cursor:
        cursor.execute(sql)
    conn.commit()
    conn.close()