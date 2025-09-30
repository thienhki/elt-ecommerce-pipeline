import pandas as pd
from postgres_operator import PostgresOperators


def transform_fact_orders():
    staging = PostgresOperators("postgres")
    warehouse = PostgresOperators("postgres")

    # -------------------------
    # Load từ staging & warehouse
    # -------------------------
    df_orders = staging.get_data_to_pd("SELECT * FROM staging.stg_orders")
    df_order_items = staging.get_data_to_pd("SELECT * FROM staging.stg_order_items")
    df_order_payments = staging.get_data_to_pd("SELECT * FROM staging.stg_order_payments")

    df_customers = warehouse.get_data_to_pd("SELECT customer_key, customers_id FROM warehouse.dim_customers")
    df_products = warehouse.get_data_to_pd("SELECT product_key, product_id FROM warehouse.dim_products")
    df_sellers = warehouse.get_data_to_pd("SELECT seller_key, seller_id FROM warehouse.dim_sellers")
    df_payments = warehouse.get_data_to_pd("SELECT payment_key, payment_type, payment_installments FROM warehouse.dim_payments")

    # -------------------------
    # Ép kiểu key để tránh lỗi merge
    # -------------------------
    df_orders["order_id"] = df_orders["order_id"].astype(str)
    df_order_items["order_id"] = df_order_items["order_id"].astype(str)
    df_order_payments["order_id"] = df_order_payments["order_id"].astype(str)

    df_orders["customers_id"] = df_orders["customers_id"].astype(str)
    df_customers["customers_id"] = df_customers["customers_id"].astype(str)

    df_order_items["product_id"] = df_order_items["product_id"].astype(str)
    df_products["product_id"] = df_products["product_id"].astype(str)

    df_order_items["seller_id"] = df_order_items["seller_id"].astype(str)
    df_sellers["seller_id"] = df_sellers["seller_id"].astype(str)

    df_order_payments["payment_type"] = df_order_payments["payment_type"].astype(str)
    df_payments["payment_type"] = df_payments["payment_type"].astype(str)
    df_order_payments["payment_installments"] = df_order_payments["payment_installments"].astype(int)
    df_payments["payment_installments"] = df_payments["payment_installments"].astype(int)

    # -------------------------
    # Merge
    # -------------------------
    df = pd.merge(df_orders, df_order_items, on="order_id", how="left")
    df = pd.merge(df, df_order_payments, on="order_id", how="left")
    df = pd.merge(df, df_customers, on="customers_id", how="left")
    df = pd.merge(df, df_sellers, on="seller_id", how="left")
    df = pd.merge(df, df_products, on="product_id", how="left")
    df = pd.merge(df, df_payments, on=["payment_type", "payment_installments"], how="left")

    # -------------------------
    # Clean & transform
    # -------------------------
    df['order_status'] = df['order_status'].str.lower()

    for col in [
        "order_purchase_timestamp", "order_approved_at",
        "order_delivered_carrier_date", "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ]:
        df[col] = pd.to_datetime(df[col])

    df['total_amount'] = df['price'] + df['freight_value']
    df['delivery_time'] = (df['order_delivered_customer_date'] - df['order_purchase_timestamp']).dt.total_seconds() / 86400
    df['estimated_delivery_time'] = (df['order_estimated_delivery_date'] - df['order_purchase_timestamp']).dt.total_seconds() / 86400

    # Date key (YYYYMMDD int)
    df['order_date_key'] = df['order_purchase_timestamp'].dt.strftime("%Y%m%d").astype(int)

    # -------------------------
    # Chọn cột fact
    # -------------------------
    fact_columns = [
        "order_id", "customer_key", "product_key", "seller_key",
        "payment_key", "order_date_key", "order_status", "price", "freight_value",
        "total_amount", "payment_value", "delivery_time", "estimated_delivery_time"
    ]
    df_fact = df[fact_columns]

    # -------------------------
    # Save
    # -------------------------
    warehouse.create_schema_if_not_exists("warehouse")
    warehouse.save_data_to_postgres("fact_orders", df_fact, index=False, schema="warehouse", if_exists="replace")

    print("✅ Đã transform và lưu dữ liệu vào fact_orders")