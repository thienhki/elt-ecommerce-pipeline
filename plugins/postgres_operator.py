from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
from sqlalchemy import create_engine
import pandas as pd

class PostgresOperators:
    def __init__(self, conn_id):
        # Chỉ lưu conn_id, không mở kết nối ngay
        self.conn_id = conn_id
        self.postgreshook = None
        self.postgres_conn = None
        self.engine = None

    def get_conn(self):
        """Khởi tạo PostgresHook và connection khi cần"""
        if not self.postgreshook:
            try:
                self.postgreshook = PostgresHook(postgres_conn_id=self.conn_id)
                self.postgres_conn = self.postgreshook.get_conn()
                self.engine = create_engine(self.postgreshook.get_uri())
            except Exception as e:
                logging.error(f"Error connecting to Postgres: {e}")
                raise e
        return self.postgres_conn

    def get_data_to_pd(self, query):
        if not self.postgreshook:
            self.get_conn()
        return self.postgreshook.get_pandas_df(query)
    
    def create_schema_if_not_exists(self, schema):
        conn = self.get_conn()
        query = f"CREATE SCHEMA IF NOT EXISTS {schema};"
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
        cur.close()
        print(f"Schema {schema} ensured exists")

    def save_data_to_postgres(self, table_name, df, index=False, schema='public', if_exists='replace'):
        if not self.postgreshook:
            self.get_conn()
        try:
            df.to_sql(table_name, self.engine, schema=schema, if_exists=if_exists, index=index)
            print(f"Saved {len(df)} records into {schema}.{table_name} successfully")
        except Exception as e:
            logging.error(f"Error saving DataFrame to {table_name}: {e}", exc_info=True)

    def execute_query(self, query):
        conn = self.get_conn()
        try:
            cur = conn.cursor()
            cur.execute(query)
            conn.commit()
            cur.close()
            logging.info(f"Query executed successfully: {query}")
        except Exception as e:
            logging.error(f"Error executing query: {e}", exc_info=True)
