from postgres_operator import PostgresOperators

def transform_dim_payments():
    staging_operator = PostgresOperators('postgres')
    warehouse_operator = PostgresOperators('postgres')

    df = staging_operator.get_data_to_pd("SELECT * FROM staging.stg_order_payments")
    df['payment_type'] = df['payment_type'].str.lower()
    df['payment_installments'].fillna(1).astype(int)

    df['payment_key'] = df.index + 1

    df = df.drop_duplicates(subset=['payment_type', 'payment_installments'])

    warehouse_operator.create_schema_if_not_exists('warehouse')
    warehouse_operator.excute_query("TRUNCATE TABLE warehouse.dim_payments")
    warehouse_operator.save_data_to_postgres(
        'dim_payments',
        df,
        index=False,
        schema='warehouse',
        if_exists='replace'
    )

    print("Đã transform và lưu dữ liệu vào dim_payments")
    