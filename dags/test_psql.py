from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator


def manipulate_postgres_data():
    # PostgresHook을 사용하여 설정된 연결 정보를 사용
    postgres_hook = PostgresHook(postgres_conn_id="postgres_conn")
    conn = postgres_hook.get_conn()
    cur = conn.cursor()

    # 쿼리 실행: 기존 테이블에서 데이터를 선택하여 새로운 테이블을 생성
    create_table_query = """
        CREATE TABLE IF NOT EXISTS new_table AS
        SELECT * FROM public.djan_movie_all
        ORDER BY movie_code ASC;
    """
    cur.execute(create_table_query)

    # 변경 사항 커밋 및 연결 종료
    conn.commit()
    cur.close()
    conn.close()


# 기본 인수 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG 정의
dag = DAG(
    "postgres_manipulation",
    default_args=default_args,
    description="A simple DAG to manipulate PostgreSQL data",
    schedule_interval=timedelta(days=1),
)

# 작업 정의
t1 = PythonOperator(
    task_id="manipulate_data",
    python_callable=manipulate_postgres_data,
    dag=dag,
)
