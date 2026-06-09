from airflow.providers.postgres.hooks.postgres import PostgresHook

def dl_load_into_dds():
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