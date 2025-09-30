from postgres_operator import PostgresOperators

def transform_dim_payments():
    staging = PostgresOperators("postgres")
    warehouse = PostgresOperators("postgres")

    df = staging.get_data_to_pd("SELECT * FROM staging.stg_order_payments")
    df['payment_type'] = df['payment_type'].str.lower()

    df = df.drop_duplicates(subset=['payment_type', 'payment_installments']).reset_index(drop=True)
    df['payment_key'] = df.index + 1

    warehouse.create_schema_if_not_exists("warehouse")
    warehouse.execute_query("TRUNCATE TABLE warehouse.dim_payments")
    warehouse.save_data_to_postgres("dim_payments", df, index=False, schema="warehouse", if_exists="replace")

    print("✅ Đã transform và lưu dữ liệu vào dim_payments")
    