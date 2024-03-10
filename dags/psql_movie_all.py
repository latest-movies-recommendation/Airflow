from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator


# 이건 테스트용 문구 이것도 추가 이것까지
def manipulate_postgres_data():
    # PostgresHook을 사용하여 설정된 연결 정보를 사용
    postgres_hook = PostgresHook(postgres_conn_id="postgres_conn")
    conn = postgres_hook.get_conn()
    cur = conn.cursor()

    # 쿼리 실행: 기존 테이블에서 데이터를 선택하여 새로운 테이블을 생성 ..
    query = """
-- djan_movie_all 제작 쿼리
        INSERT INTO djan_movie_all
        SELECT
            A.moviecd AS movie_code,
            CAST(A.rank as integer) as rank,
            CAST(A.rankinten AS INTEGER) AS rank_intensity,
            A.movienm AS korean_name,
            A.opendt AS open_date,
            A.audiacc,
            CAST(A.audicnt AS INTEGER) / NULLIF(CAST(A.showcnt AS INTEGER), 0) AS audicnt_showcnt,
            B.genre, B.running_time
        from daily_box_office as A
        join djan_movie_info as B on B.moviecd=A.moviecd
    """
    cur.execute(query)

    # 변경 사항 커밋 및 연결 종료
    conn.commit()
    cur.close()
    conn.close()


# 기본 인수 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 3),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG 정의
dag = DAG(
    "psql_movie_all",
    default_args=default_args,
    description="A simple DAG to manipulate PostgreSQL data",
    schedule_interval="0 0 1 * *",
)

# 작업 정의
t1 = PythonOperator(
    task_id="psql_movie_all",
    python_callable=manipulate_postgres_data,
    dag=dag,
)