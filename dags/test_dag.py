# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.providers.mysql.hooks.mysql import MySqlHook
# from airflow.providers.postgres.hooks.postgres import PostgresHook
# from datetime import datetime


# def test_mysql_connection():
#     hook = MySqlHook(mysql_conn_id="mysql")  # ID connection bạn tạo trong Airflow
#     conn = hook.get_conn()
#     cursor = conn.cursor()
#     cursor.execute("SELECT 1;")
#     result = cursor.fetchone()
#     print("MySQL test result:", result)


# def test_postgres_connection():
#     hook = PostgresHook(postgres_conn_id="postgres")  # ID connection bạn tạo trong Airflow
#     conn = hook.get_conn()
#     cursor = conn.cursor()
#     cursor.execute("SELECT 1;")
#     result = cursor.fetchone()
#     print("Postgres test result:", result)


# with DAG(
#     dag_id="test_connections",
#     start_date=datetime(2025, 9, 22),
#     schedule_interval=None,
#     catchup=False,
#     tags=["test"],
# ) as dag:

#     test_mysql = PythonOperator(
#         task_id="test_mysql_conn",
#         python_callable=test_mysql_connection,
#     )

#     test_postgres = PythonOperator(
#         task_id="test_postgres_conn",
#         python_callable=test_postgres_connection,
#     )

#     test_mysql >> test_postgres

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

# Import các class operator đã refactor
from mysql_operator import MySQLOperators
from postgres_operator import PostgresOperators

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 26),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

def test_mysql_connection(**kwargs):
    try:
        mysql_op = MySQLOperators(conn_id='mysql')
        # Lấy dữ liệu 1 dòng test
        result = mysql_op.get_records("SELECT 1;")
        print("MySQL test result:", result)
    except Exception as e:
        logging.error(f"MySQL connection failed: {e}")

def test_postgres_connection(**kwargs):
    try:
        pg_op = PostgresOperators(conn_id='postgres')
        result = pg_op.get_data_to_pd("SELECT 1;")
        print("Postgres test result:", result)
    except Exception as e:
        logging.error(f"Postgres connection failed: {e}")

with DAG(
    'test_db_connections',
    default_args=default_args,
    description='Test MySQL and Postgres connections',
    schedule_interval=None,  # Chạy manual
    catchup=False
) as dag:

    task_test_mysql = PythonOperator(
        task_id='test_mysql_connection',
        python_callable=test_mysql_connection
    )

    task_test_postgres = PythonOperator(
        task_id='test_postgres_connection',
        python_callable=test_postgres_connection
    )

    # Đặt thứ tự nếu muốn, hoặc chạy song song
    task_test_mysql >> task_test_postgres
