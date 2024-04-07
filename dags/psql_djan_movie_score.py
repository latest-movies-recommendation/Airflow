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

    # 쿼리 실행: 기존 테이블에서 데이터를 선택하여 새로운 테이블을 생성
    query = """
        -- 일일 평점정보
        -- TRUNCATE TABLE djan_movie_rating;
        INSERT INTO djan_movie_score (moviecd, movienm, naver_rating, naver_male, naver_female, naver_critics, watcha_rating)
        select movie_code as moviecd, movie_name as movienm , naver_rating, naver_male, naver_female,naver_critics, watcha_rating
        from daily_movie_ratings
        ON CONFLICT (moviecd)
        DO UPDATE SET
            movienm = EXCLUDED.movienm,
            naver_rating = EXCLUDED.naver_rating,
            naver_male = EXCLUDED.naver_male,
            naver_female = EXCLUDED.naver_female,
            naver_critics = EXCLUDED.naver_critics,
            watcha_rating = EXCLUDED.watcha_rating;
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
    "psql_djan_movie_score",
    default_args=default_args,
    description="A simple DAG to manipulate PostgreSQL data",
    schedule_interval="0 12 * * *",
)

# 작업 정의
t1 = PythonOperator(
    task_id="manipulate_data",
    python_callable=manipulate_postgres_data,
    dag=dag,
)
