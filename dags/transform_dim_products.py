from postgres_operator import PostgresOperators
import pandas as pd
def transform_dim_products():
    staging_operator = PostgresOperators('postgres')
    warehouse_operator = PostgresOperators('postgres')

    df_products = staging_operator.get_data_to_pd("SELECT * FROM staging.stg_products")
    df_categories = staging_operator.get_data_to_pd("SELECT * FROM staging.stg_product_category_name_translation")

    df = pd.merge(df_products, df_categories, on='product_category_name', how='left')

        # Numeric: thay vì 0, dùng -1 để đánh dấu missing
    df['product_weight_g'] = df['product_weight_g'].fillna(-1)
    df['product_length_cm'] = df['product_length_cm'].fillna(-1)
    df['product_height_cm'] = df['product_height_cm'].fillna(-1)
    df['product_width_cm'] = df['product_width_cm'].fillna(-1)

    # Các cột độ dài / số lượng ảnh: null thì để 0 (ý nghĩa: chưa có dữ liệu)
    df['product_name_lenght'] = df['product_name_lenght'].fillna(0)
    df['product_description_lenght'] = df['product_description_lenght'].fillna(0)
    df['product_photos_qty'] = df['product_photos_qty'].fillna(0)

    # Danh mục: nếu không join được thì Unknown
    df['product_category_name_english'] = df['product_category_name_english'].fillna('Unknown')

       # Tạo surrogate key
    df['product_key'] = df.index + 1
    
    # Thêm cột để theo dõi thay đổi (SCD Type 1)
    df['last_updated'] = pd.Timestamp.now().date()
    
    # Lưu dữ liệu vào bảng dim_products
    warehouse_operator.create_schema_if_not_exists('warehouse')
    warehouse_operator.excute_query("TRUNCATE TABLE warehouse.dim_products")
    warehouse_operator.save_data_to_postgres(
        'dim_products',
        df,
        index=False,
        schema='warehouse',
        if_exists='replace'
    )
     
    
    print("Đã transform và lưu dữ liệu vào dim_products")

