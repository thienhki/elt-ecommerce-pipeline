from postgres_operator import PostgresOperators
import pandas as pd
from datetime import datetime, timedelta

def transform_dim_dates():
    warehouse_operator = PostgresOperators('postgres')

    # 1. Tao khoang thoi gian (2016 - 2026)
    start_date = datetime(2016, 1, 1)
    end_date = datetime(2026, 12, 31)
    date_range = pd.date_range(start=start_date, end=end_date)

    # 2. Tao dataframe dime_date
    df = pd.DataFrame(
        {
            # Surrogate key
            'data_key' : date_range.strftime('%Y%m%d').astype(int),
            'full_date' : date_range,
            'day' : date_range.day,
            'month' : date_range.month,
            'year' : date_range.year,
            'quarter' : date_range.quarter,
            'day_of_week' : date_range.dayofweek + 1,
            'day_name' : date_range.strftime('%A'),
            'month_name' : date_range.strftime('%B'),
            'is_weekend' : date_range.dayofweek.isin([5, 6])
        }
    )

    #3. Lưu vào datawarehouse
    warehouse_operator.create_schema_if_not_exists('warehouse')
    warehouse_operator.excute_query("TRUNCATE TABLE warehouse.dim_dates")
    warehouse_operator.save_data_to_postgres('dim_dates', df, False, schema='warehouse', if_exists='replace')

    print("Đã transform và lưu dữ liệu vào dim_dates")