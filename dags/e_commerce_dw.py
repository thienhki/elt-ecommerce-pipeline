from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

from extract_data import extract_and_load_to_staging
from transform_dim_customer import transform_dim_customer
from transform_dim_products import transform_dim_products
from transform_dim_sellers import transform_dim_sellers
from transform_dim_geolocation import transform_dim_geolocation
from transform_dim_date import transform_dim_dates
from transform_dim_payment import transform_dim_payments
from transform_fact_orders import transform_fact_orders


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 26),
    'end_date': datetime(2024, 10, 29),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'e_commerce_dw_etl',
    default_args=default_args,
    description='ETL process for E-commerce Data Warehouse',
    schedule_interval="@daily",
) as dag:

    with TaskGroup("extract") as extract_group:
        extract_task = PythonOperator(
            task_id='extract_and_load_data_to_staging',
            python_callable=extract_and_load_to_staging
        )

    with TaskGroup("transform_dims") as transform_dims_group:
        task_dim_customers = PythonOperator(
            task_id='transform_dim_customers',
            python_callable=transform_dim_customer
        )
        task_dim_products = PythonOperator(
            task_id='transform_dim_products',
            python_callable=transform_dim_products
        )
        task_dim_sellers = PythonOperator(
            task_id='transform_dim_sellers',
            python_callable=transform_dim_sellers
        )
        task_dim_geolocation = PythonOperator(
            task_id='transform_dim_geolocation',
            python_callable=transform_dim_geolocation
        )
        task_dim_dates = PythonOperator(
            task_id='transform_dim_dates',
            python_callable=transform_dim_dates
        )
        task_dim_payments = PythonOperator(
            task_id='transform_dim_payments',
            python_callable=transform_dim_payments
        )

    with TaskGroup("Load") as load_group:
        task_fact_orders = PythonOperator(
            task_id='load_fact_orders',
            python_callable=transform_fact_orders
        )

    extract_group >> transform_dims_group
    [
        task_dim_customers,
        task_dim_products,
        task_dim_sellers,
        task_dim_geolocation,
        task_dim_dates,
        task_dim_payments
    ] >> task_fact_orders
