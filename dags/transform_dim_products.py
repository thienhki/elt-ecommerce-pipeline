from postgres_operator import PostgresOperators
import pandas as pd

def transform_dim_products():
    staging = PostgresOperators("postgres")
    warehouse = PostgresOperators("postgres")

    df_products = staging.get_data_to_pd("SELECT * FROM staging.stg_products")
    df_categories = staging.get_data_to_pd("SELECT * FROM staging.stg_product_category_name_translation")

    df = pd.merge(df_products, df_categories, on="product_category_name", how="left")

    # Fill NA
    df['product_weight_g'] = df['product_weight_g'].fillna(-1)
    df['product_length_cm'] = df['product_length_cm'].fillna(-1)
    df['product_height_cm'] = df['product_height_cm'].fillna(-1)
    df['product_width_cm'] = df['product_width_cm'].fillna(-1)

    df['product_name_lenght'] = df['product_name_lenght'].fillna(0)
    df['product_description_lenght'] = df['product_description_lenght'].fillna(0)
    df['product_photos_qty'] = df['product_photos_qty'].fillna(0)

    df['product_category_name_english'] = df['product_category_name_english'].fillna("Unknown")

    df = df.reset_index(drop=True)
    df['product_key'] = df.index + 1
    df['last_updated'] = pd.Timestamp.now().date()

    warehouse.create_schema_if_not_exists("warehouse")
    warehouse.execute_query("TRUNCATE TABLE warehouse.dim_products")
    warehouse.save_data_to_postgres("dim_products", df, index=False, schema="warehouse", if_exists="replace")

    print("✅ Đã transform và lưu dữ liệu vào dim_products")
