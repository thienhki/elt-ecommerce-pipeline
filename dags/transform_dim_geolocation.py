from postgres_operator import PostgresOperators
import pandas as pd

def transform_dim_geolocation():
    staging_operator = PostgresOperators('postgres')
    warehouse_operator = PostgresOperators('postgres')

    query = 'SELECT * FROM staging.stg_geolocation'
    df = staging_operator.get_data_to_pd(query)
    
    df['geolocation_zip_code_prefix'] = df['geolocation_zip_code_prefix'].astype(str).str.zfill(5)
    df['geolocation_city'] = df['geolocation_city'].str.title()
    df['geolocation_state'] = df['geolocation_state'].str.upper()

    df['geolocation_key'] = df.index + 1
    warehouse_operator.create_schema_if_not_exists('warehouse')

    warehouse_operator.excute_query("TRUNCATE TABLE warehouse.dim_geolocation")

    warehouse_operator.save_data_to_postgres('dim_geolocation', df, False, schema = 'warehouse', if_exists='replace')

    print("Đã transform và lưu trữ dữ liệu vào dim_geolocation")