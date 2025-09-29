import pandas as pd
from postgres_operator import PostgresOperators
from datetime import datetime, timedelta

def transform_dim_customer():
    staging_operator = PostgresOperators("postgres")
    warehouse_operator = PostgresOperators("postgres")

    # 1.Đọc dữ liệu từ staging
    query = "SELECT * FROM staging.stg_customers"
    df = staging_operator.get_data_to_pd(query)
    # 2.Làm sạch và chuẩn hóa dữ liệu
    df['customers_id'] = df['customers_id'].astype(str)
    df['customers_zip_code_prefix'] = df['customers_zip_code_prefix'].astype(str).str.zfill(5)
    df['customers_city'] = df['customers_city'].str.title()
    df['customers_state'] = df['customers_state'].str.upper()

    #3. Tạo surrogate key
    df['customer_key'] = df.index + 1
    
    #4. Thêm cột 
    current_date = datetime.now().date()
    future_date = '9999-12-31'

    df['effective_date'] = current_date
    df['end_date'] = future_date
    df['is_current'] = True
    
    #5. Lưu dữ liệu vào bảng dim_customers
    warehouse_operator.create_schema_if_not_exists('warehouse')
    warehouse_operator.excute_query("TRUNCATE TABLE warehouse.dim_customers")
    warehouse_operator.save_data_to_postgres('dim_customers', df, False, schema='warehouse',if_exists='replace')

    print("Đã Transform và lưu trữ dữ liệu vào dim_customers")
    
 
