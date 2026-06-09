from airflow.providers.postgres.hooks.postgres import PostgresHook

from helpers.get_ch_client import get_ch_client

def cl_sales_performance_dtm():
    hook = PostgresHook(postgres_conn_id="warehouse_postgres_conn")
    conn = get_ch_client()
    ch_cursor = conn.cursor()

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
    
    ch_cursor.execute("TRUNCATE TABLE dm_datamart_1_sales_by_class_date")
    ch_cursor.executemany("""
        INSERT INTO dm_datamart_1_sales_by_class_date
        VALUES
    """, rows)
    ch_cursor.close()
    conn.close()


def cl_customer_behavior_dtm():
    hook = PostgresHook(postgres_conn_id="warehouse_postgres_conn")
    conn = get_ch_client()
    ch_cursor = conn.cursor()

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



def cl_discount_effectiveness_dtm():
    hook = PostgresHook(postgres_conn_id="warehouse_postgres_conn")
    conn = get_ch_client()
    ch_cursor = conn.cursor()

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
