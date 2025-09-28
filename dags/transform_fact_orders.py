import pandas as pd
from postgres_operator import PostgresOperators


def transform_fact_orders():
    staging_operator = PostgresOperators('postgres')
    warehouse_operator = PostgresOperators('postgres')
    
    # Đọc dữ liệu từ staging
    df_orders = staging_operator.get_data_to_pd("SELECT * FROM staging.stg_orders")
    df_order_items = staging_operator.get_data_to_pd("SELECT * FROM staging.stg_order_items")
    df_order_payments = staging_operator.get_data_to_pd("SELECT * FROM staging.stg_order_payments")
    df_customers = staging_operator.get_data_to_pd("SELECT customers_id, customers_zip_code_prefix FROM staging.stg_customers")
    
    # Kết hợp dữ liệu
    df = pd.merge(df_orders, df_order_items, on='order_id', how='left')
    df = pd.merge(df, df_order_payments, on='order_id', how='left')
    df = pd.merge(df, df_customers, on='customers_id', how='left')
    
    # Kiểm tra các cột trong DataFrame
    print("Các cột trong DataFrame sau khi merge:")
    print(df.columns)
    
    # Transform và làm sạch dữ liệu
    df['order_status'] = df['order_status'].str.lower()
    df['order_purchase_timestamp'] = pd.to_datetime(df['order_purchase_timestamp'])
    df['order_approved_at'] = pd.to_datetime(df['order_approved_at'])
    df['order_delivered_carrier_date'] = pd.to_datetime(df['order_delivered_carrier_date'])
    df['order_delivered_customer_date'] = pd.to_datetime(df['order_delivered_customer_date'])
    df['order_estimated_delivery_date'] = pd.to_datetime(df['order_estimated_delivery_date'])
    
    # Tính toán các metrics
    df['total_amount'] = df['price'] + df['freight_value']
    df['delivery_time'] = (df['order_delivered_customer_date'] - df['order_purchase_timestamp']).dt.total_seconds() / 86400
    df['estimated_delivery_time'] = (df['order_estimated_delivery_date'] - df['order_purchase_timestamp']).dt.total_seconds() / 86400
    
    # Tạo các foreign keys
    df['customer_key'] = df['customers_id']
    df['product_key'] = df['product_id']
    df['seller_key'] = df['seller_id']
    
    # Kiểm tra xem cột customers_zip_code_prefix có tồn tại không
    if 'customers_zip_code_prefix' in df.columns:
        df['geolocation_key'] = df['customers_zip_code_prefix']
    else:
        print("Cột customers_zip_code_prefix không tồn tại. Sử dụng giá trị mặc định.")
        df['geolocation_key'] = 'unknown'
    
    df['payment_key'] = df['payment_type'].astype('category').cat.codes + 1
    df['order_date_key'] = df['order_purchase_timestamp'].dt.date
    
    # Chọn các cột cần thiết cho bảng fact
    fact_columns = ['order_id', 'customer_key', 'product_key', 'seller_key', 'geolocation_key', 'payment_key', 'order_date_key',
                    'order_status', 'price', 'freight_value', 'total_amount', 'payment_value',
                    'delivery_time', 'estimated_delivery_time']
    
    df_fact = df[fact_columns]
    data = [tuple(row) for row in df_fact.to_numpy()]
    # Lưu dữ liệu vào bảng fact_orders
    warehouse_operator.save_data_to_postgres(
        'fact_orders',
        df_fact,
        index=False,
        schema='warehouse',
        if_exists='replace'
    )
    
    print("Đã transform và lưu dữ liệu vào fact_orders")