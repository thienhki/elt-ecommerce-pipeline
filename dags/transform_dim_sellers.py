from postgres_operator import PostgresOperators
import pandas as pd

def transform_dim_sellers():
    staging_operator = PostgresOperators('postgres')
    warehouse_operator = PostgresOperators('postgres')
    
    # 1. Đọc dữ liệu từ staging
    df = staging_operator.get_data_to_pd("SELECT * FROM staging.stg_sellers")
    
    # 2. Transform và làm sạch dữ liệu
    df['seller_zip_code_prefix'] = df['seller_zip_code_prefix'].astype(str).str.zfill(5)
    df['seller_city'] = df['seller_city'].str.title()
    df['seller_state'] = df['seller_state'].str.upper()
    
    # 3. Tạo surrogate key
    df['seller_key'] = df.index + 1
    
    # 4. Thêm cột để theo dõi thay đổi (Type 1)
    df['last_updated'] = pd.Timestamp.now().date()
    
    
    # 6. Truncate trước khi insert (Type 1)
    warehouse_operator.excute_query("TRUNCATE TABLE warehouse.dim_sellers")
    
    # 7. Insert vào warehouse
    warehouse_operator.save_data_to_postgres(
        table_name='dim_sellers',
        df=df,
        index=False,
        schema='warehouse',
        if_exists='replace'
    )
    
    print("✅ Đã transform và lưu dữ liệu vào dim_sellers (Type 1)")
