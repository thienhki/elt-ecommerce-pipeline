from postgres_operator import PostgresOperators
import pandas as pd
from datetime import datetime, timedelta

def transform_dim_dates():
    warehouse = PostgresOperators("postgres")

    start_date = datetime(2016, 1, 1)
    end_date = datetime(2026, 12, 31)
    date_range = pd.date_range(start=start_date, end=end_date)

    df = pd.DataFrame({
        "date_key": date_range.strftime("%Y%m%d").astype(int),  # surrogate key dạng int
        "date": date_range,
        "day": date_range.day,
        "month": date_range.month,
        "year": date_range.year,
        "quarter": date_range.quarter,
        "day_of_week": date_range.dayofweek + 1,
        "day_name": date_range.strftime("%A"),
        "month_name": date_range.strftime("%B"),
        "is_weekend": date_range.dayofweek.isin([5, 6])
    })

    warehouse.create_schema_if_not_exists("warehouse")
    warehouse.execute_query("TRUNCATE TABLE warehouse.dim_dates")
    warehouse.save_data_to_postgres("dim_dates", df, index=False, schema="warehouse", if_exists="replace")

    print("✅ Đã transform và lưu dữ liệu vào dim_dates")