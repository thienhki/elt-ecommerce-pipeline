from postgres_operator import PostgresOperators
import pandas as pd

def transform_dim_sellers():
    staging = PostgresOperators("postgres")
    warehouse = PostgresOperators("postgres")

    df = staging.get_data_to_pd("SELECT * FROM staging.stg_sellers")

    df['seller_zip_code_prefix'] = df['seller_zip_code_prefix'].astype(str).str.zfill(5)
    df['seller_city'] = df['seller_city'].str.title()
    df['seller_state'] = df['seller_state'].str.upper()

    df = df.reset_index(drop=True)
    df['seller_key'] = df.index + 1
    df['last_updated'] = pd.Timestamp.now().date()

    warehouse.create_schema_if_not_exists("warehouse")
    warehouse.execute_query("TRUNCATE TABLE warehouse.dim_sellers")
    warehouse.save_data_to_postgres("dim_sellers", df, index=False, schema="warehouse", if_exists="replace")

    print("✅ Đã transform và lưu dữ liệu vào dim_sellers")
