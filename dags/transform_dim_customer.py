import pandas as pd
from postgres_operator import PostgresOperators
from datetime import datetime, date

def transform_dim_customer():
    staging = PostgresOperators("postgres")
    warehouse = PostgresOperators("postgres")

    df = staging.get_data_to_pd("SELECT * FROM staging.stg_customers")

    df['customers_id'] = df['customers_id'].astype(str)
    df['customers_zip_code_prefix'] = df['customers_zip_code_prefix'].astype(str).str.zfill(5)
    df['customers_city'] = df['customers_city'].str.title()
    df['customers_state'] = df['customers_state'].str.upper()

    df = df.reset_index(drop=True)
    df['customer_key'] = df.index + 1

    # SCD Type 2 (effective_date – end_date)
    current_date = datetime.now().date()
    df['effective_date'] = current_date
    df['end_date'] = date(9999, 12, 31)   # dùng datetime.date
    df['is_current'] = True

    warehouse.create_schema_if_not_exists("warehouse")
    warehouse.execute_query("TRUNCATE TABLE warehouse.dim_customers")
    warehouse.save_data_to_postgres("dim_customers", df, index=False, schema="warehouse", if_exists="replace")

    print("✅ Đã transform và lưu dữ liệu vào dim_customers")
 
