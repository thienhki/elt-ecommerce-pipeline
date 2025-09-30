from postgres_operator import PostgresOperators
import pandas as pd

def transform_dim_geolocation():
    staging = PostgresOperators("postgres")
    warehouse = PostgresOperators("postgres")

    df = staging.get_data_to_pd("SELECT * FROM staging.stg_geolocation")

    df['geolocation_zip_code_prefix'] = df['geolocation_zip_code_prefix'].astype(str).str.zfill(5)
    df['geolocation_city'] = df['geolocation_city'].str.title()
    df['geolocation_state'] = df['geolocation_state'].str.upper()

    df = df.reset_index(drop=True)
    df['geolocation_key'] = df.index + 1

    warehouse.create_schema_if_not_exists("warehouse")
    warehouse.execute_query("TRUNCATE TABLE warehouse.dim_geolocation")
    warehouse.save_data_to_postgres("dim_geolocation", df, index=False, schema="warehouse", if_exists="replace")

    print("✅ Đã transform và lưu dữ liệu vào dim_geolocation")