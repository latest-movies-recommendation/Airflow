import logging
from datetime import date, datetime, timedelta
from io import StringIO

import pandas as pd
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python import PythonOperator


def daily_filename():
    day = date.today() - timedelta(1)
    ymd = day.strftime("%Y%m%d")

    return f"kofic/daily-box-office/{ymd}.csv"


def s3_to_ranking_list():
    try:
        s3 = S3Hook(aws_conn_id="aws_conn")
        obj = s3.get_key(
            key=daily_filename(), bucket_name=Variable.get("s3_bucket_name")
        )
        if obj:
            csv_data = obj.get()["Body"].read().decode("utf-8")
            df = pd.read_csv(StringIO(csv_data))

            # 영진위 1-10위 영화
            movies_list = df.iloc[-10:]
            logging.info(movies_list)
            return movies_list
        else:
            return None
    except Exception as e:
        logging.info(f"오류 발생: {e}")
        return []


def list_to_postgres(**kwargs):

    # movie_ranking_ten은 dataframe
    movie_ranking_ten = kwargs["ti"].xcom_pull(task_ids="s3_to_ranking_list")
    data = movie_ranking_ten.where(pd.notnull(movie_ranking_ten), None)

    # PostgreSQL Hook을 사용하여 연결
    postgres_hook = PostgresHook(postgres_conn_id="postgres_conn")
    conn = postgres_hook.get_conn()
    cur = conn.cursor()

    try:
        # 테이블이 이미 존재하는지 확인
        check_table_query = "SELECT to_regclass('daily_box_office')"
        cur.execute(check_table_query)
        result = cur.fetchone()[0]

        if result is None:
            # 테이블이 존재하지 않으면 새로운 테이블 생성
            create_table_query = f'CREATE TABLE daily_box_office ({", ".join(f"{col} VARCHAR" for col in data.columns)})'
            cur.execute(create_table_query)
            logging.info("Table daily_box_office created.")

        # 테이블에 데이터가 있으면 지우고 삽입하도록 설정
        if not data.empty:
            delete_query = "DELETE FROM daily_box_office"
            cur.execute(delete_query)

        insert_query = f"INSERT INTO daily_box_office ({', '.join(data.columns)}) VALUES ({', '.join(['%s'] * len(data.columns))})"
        for _, row in data.iterrows():
            cur.execute(insert_query, tuple(row))
            logging.info("SQL insert start")

        conn.commit()
        logging.info("Data successfully inserted into PostgreSQL RDS.")
    except Exception as e:
        logging.error(f"Error: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()


default_args = {
    "start_date": datetime(2024, 2, 28),
    "retries": 1,
    "retry_delay": timedelta(minutes=20),
}

with DAG(
    dag_id="daily_box_office_rds",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
) as dag:

    trigger_daily_office_task = TriggerDagRunOperator(
        task_id="trigger_daily_office_task",
        trigger_dag_id="daily_box_office_elt",
    )

    s3_to_ranking_list = PythonOperator(
        task_id="s3_to_ranking_list", python_callable=s3_to_ranking_list, dag=dag
    )
    list_to_postgres = PythonOperator(
        task_id="list_to_postgres", python_callable=list_to_postgres, dag=dag
    )
    trigger_daily_office_task >> s3_to_ranking_list
    s3_to_ranking_list >> list_to_postgres
