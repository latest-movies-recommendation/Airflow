from datetime import datetime, timedelta
from io import StringIO

import pandas as pd
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python_operator import PythonOperator


def load_csv_to_postgres():
    s3_hook = S3Hook(aws_conn_id="aws_conn")
    bucket_name = "s3_bucket_name"  # Airflow Variables에서 설정하거나 직접 입력
    prefix = "kofic/movie/directors/"
    keys = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix)
    postgres_hook = PostgresHook(postgres_conn_id="postgres_conn")

    insert_sql = (
        "INSERT INTO directors (movieCd, peopleNm, peopleNmEn) "
        "VALUES (%s, %s, %s) ON CONFLICT (movieCd) DO NOTHING"
    )

    with postgres_hook.get_conn() as conn:
        with conn.cursor() as cur:
            for key in keys:
                if key.endswith(".csv"):  # 파일이 CSV 형식인지 확인
                    s3_obj = s3_hook.get_key(key, bucket_name=bucket_name)
                    s3_obj_content = s3_obj.get()["Body"].read().decode("utf-8")
                    csv_data = StringIO(s3_obj_content)
                    df = pd.read_csv(csv_data)

                    # DataFrame의 각 행을 반복하여 데이터 삽입
                    for index, row in df.iterrows():
                        cur.execute(
                            insert_sql,
                            (row["movieCd"], row["peopleNm"], row["peopleNmEn"]),
                        )

            conn.commit()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 3),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "z_s3_to_postgres_dag",
    default_args=default_args,
    description="Load CSV files from S3 to PostgreSQL",
    schedule_interval=timedelta(days=1),
    catchup=False,
)

load_task = PythonOperator(
    task_id="load_csv_to_postgres",
    python_callable=load_csv_to_postgres,
    dag=dag,
)
