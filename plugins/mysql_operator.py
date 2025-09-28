from airflow.providers.mysql.hooks.mysql import MySqlHook
import logging
from support_processing import TemplateOperatorDB
from contextlib import closing
import pandas as pd
import csv

class MySQLOperators:
    def __init__(self, conn_id="mysql"):
        # Chỉ lưu conn_id, không mở kết nối ngay
        self.conn_id = conn_id
        self.mysqlhook = None
        self.mysql_conn = None

    def get_conn(self):
        """Khởi tạo MySqlHook và connection khi cần"""
        if not self.mysqlhook:
            try:
                self.mysqlhook = MySqlHook(mysql_conn_id=self.conn_id)
                self.mysql_conn = self.mysqlhook.get_conn()
            except Exception as e:
                logging.error(f"Error connecting to MySQL: {e}")
                raise e
        return self.mysql_conn

    def get_data_to_pd(self, query):
        if not self.mysqlhook:
            self.get_conn()
        return self.mysqlhook.get_pandas_df(query)

    def get_records(self, query):
        if not self.mysqlhook:
            self.get_conn()
        return self.mysqlhook.get_records(query)

    def excute_query(self, query):
        conn = self.get_conn()
        try:
            cur = conn.cursor()
            cur.execute(query)
            conn.commit()
            cur.close()
            logging.info(f"Query executed successfully: {query}")
        except Exception as e:
            logging.error(f"Error executing query: {query}", exc_info=True)
            conn.rollback()

    def insert_data_from_pd(self, dataframe, table_name, chunksize=100000):
        query = TemplateOperatorDB(table_name).create_query_insert_into(dataframe)
        conn = self.get_conn()
        if self.mysqlhook.supports_autocommit:
            self.mysqlhook.set_autocommit(conn, False)

        try:
            with closing(conn.cursor()) as cur:
                for i in range(0, len(dataframe), chunksize):
                    lst = []
                    for row in dataframe.iloc[i: i + chunksize].values.tolist():
                        sub_lst = [self.mysqlhook._serialize_cell(cell, conn) for cell in row]
                        lst.append(tuple(sub_lst))
                    cur.executemany(query, lst)
                    conn.commit()
            print(f"Insert {len(dataframe)} records into {table_name} successfully")
        except Exception as e:
            logging.error(f"Can't execute query: {query}", exc_info=True)
            conn.rollback()

    def delete_data_where_in(self, column_name, list_values, table_name):
        query = TemplateOperatorDB(table_name).delete_query_where_in(column_name, list_values)
        conn = self.get_conn()
        if self.mysqlhook.supports_autocommit:
            self.mysqlhook.set_autocommit(conn, False)

        try:
            with closing(conn.cursor()) as cur:
                lst_values = tuple(self.mysqlhook._serialize_cell(cell, conn) for cell in list_values)
                cur.execute(query, lst_values)
                delete_rows = cur.rowcount
                conn.commit()
            print(
                f"Requested to delete {len(list_values)} records, "
                f"Deleted {delete_rows} records from {table_name} successfully"
            )
        except Exception as e:
            logging.error(f"Can't execute query: {query}", exc_info=True)
            conn.rollback()

    def insert_data_into_table(self, data, table_name, create_table=None):
        conn = self.get_conn()
        if create_table:
            query = f"CREATE TABLE IF NOT EXISTS {table_name} LIKE {create_table};"
            cur = conn.cursor()
            cur.execute(query)
            conn.commit()
            cur.close()

        try:
            self.mysqlhook.insert_rows(table=table_name, rows=data)
            print(f"Insert {len(data)} records into {table_name} successfully")
        except Exception as e:
            logging.error(f"Can't insert data into {table_name}", exc_info=True)

    def remove_table_if_exist(self, table_name_del):
        query = f"DROP TABLE IF EXISTS {table_name_del};"
        conn = self.get_conn()
        try:
            cur = conn.cursor()
            cur.execute(query)
            conn.commit()
            cur.close()
            print(f"Drop table {table_name_del} successfully")
        except Exception as e:
            logging.error(f"Can't drop table {table_name_del}", exc_info=True)

    def truncate_all_data_from_table(self, table_name):
        query = f"TRUNCATE TABLE {table_name};"
        conn = self.get_conn()
        try:
            cur = conn.cursor()
            cur.execute(query)
            conn.commit()
            cur.close()
            print(f"Truncate all data from table {table_name} successfully")
        except Exception as e:
            logging.error(f"Can't truncate table {table_name}", exc_info=True)

    def dump_table_with_cursor(self, table_name, file_path, sizefetch=1000):
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name}")

        col_names = [desc[0] for desc in cursor.description]
        print("Column Names:", col_names)

        with open(file_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(col_names)
            while True:
                rows = cursor.fetchmany(sizefetch)
                if not rows:
                    break
                writer.writerows(rows)
        cursor.close()

    def load_data_from_file(self, table_name, file_path):
        conn = self.get_conn()
        try:
            cursor = conn.cursor()
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                header = next(reader)
                rows = list(reader)

            placeholders = ', '.join(['%s'] * len(header))
            columns = ', '.join(header)
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

            cursor.executemany(query, rows)
            conn.commit()
            cursor.close()
            logging.info(f"Loaded data from {file_path} to {table_name} successfully")
        except Exception as e:
            logging.error(f"Error loading data from {file_path} to {table_name}: {e}")

            


        

    
    

