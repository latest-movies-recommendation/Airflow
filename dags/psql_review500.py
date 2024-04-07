import logging
from datetime import datetime, timedelta
from io import StringIO

import pandas as pd
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


def s3_to_postgres_review500():
    # S3에서 CSV 파일 읽기
    s3 = S3Hook(aws_conn_id="aws_conn")
    bucket_name = "de-4-2-dev-bucket"
    s3_key = "naver/300reviews.csv"
    obj = s3.get_key(key=s3_key, bucket_name=bucket_name)
    if obj:
        csv_data = obj.get()["Body"].read().decode("utf-8")
        s3_dataframe = pd.read_csv(StringIO(csv_data))
        data = s3_dataframe.where(pd.notnull(s3_dataframe), None)

    # PostgreSQL에 연결
    postgres_hook = PostgresHook(postgres_conn_id="postgres_conn")
    conn = postgres_hook.get_conn()
    cur = conn.cursor()

    try:
        # 데이터 삽입
        insert_query = f"INSERT INTO review500 ({', '.join(data.columns)}) VALUES ({', '.join(['%s' for _ in data.columns])})"
        for _, row in data.iterrows():
            cur.execute(insert_query, tuple(row))

        conn.commit()
        logging.info("Data successfully inserted into review500 table in PostgreSQL.")
    except Exception as e:
        logging.error(f"Error: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()


# DAG 정의
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 3, 9),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "s3_to_postgres_review500_dag",
    default_args=default_args,
    description="Load data from S3 to PostgreSQL review500 table",
    schedule_interval="0 0 1 * *",
)

load_reviews_to_rds = PythonOperator(
    task_id="load_reviews_to_rds", python_callable=s3_to_postgres_review500, dag=dag
)

load_reviews_to_rds
