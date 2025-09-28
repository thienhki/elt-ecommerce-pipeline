from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from mysql_operator import MySQLOperators
from postgres_operator import PostgresOperators

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
        df = source_db.get_data_to_pd(f"SELECT * FROM {table}")
        target_db.save_data_to_postgres(f"stg_{table}", df, schema='staging', if_exists='replace')
        print(f"Extract and load {table} to staging successfully")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'extract_to_staging',
    default_args=default_args,
    description='Extract data from MySQL and load to Postgres staging',
    schedule_interval=timedelta(days=1),
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id='extract_and_load',
        python_callable=extract_and_load_to_staging
    )
