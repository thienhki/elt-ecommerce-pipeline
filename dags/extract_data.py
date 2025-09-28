from airflow.providers.mysql.hooks.mysql import MySqlHook
from mysql_operator import MySQLOperators
from postgres_operator import PostgresOperators

def test_mysql_connection():
    hook = MySqlHook(mysql_conn_id="mysql")  # ID connection bạn tạo trong Airflow
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT 1;")
    result = cursor.fetchone()
    print("MySQL test result:", result)

def extract_and_load_to_staging():
    source_db = MySQLOperators(conn_id='mysql')
    target_db = PostgresOperators(conn_id='postgres')
    target_db.create_schema_if_not_exists('staging')
    tables = [
        "customers",
        "geolocation",
        "order_items",
        "order_payments",
        "order_reviews",
        "orders",
        "product_category_name_translation",
        "products",
        "sellers"   
    ]

    for table in tables:
        query = f"SELECT * FROM {table}"
        df = source_db.get_data_to_pd(query)
        target_db.save_data_to_postgres(f"stg_{table}", df, index=False, schema = 'staging', if_exists='replace')
        print(f"Extract and load {table} to staging successfully")





