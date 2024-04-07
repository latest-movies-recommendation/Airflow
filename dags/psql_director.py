from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator


# 테스트용 : director 테이블 생성
def manipulate_postgres_data():
    # PostgresHook을 사용하여 설정된 연결 정보를 사용
    postgres_hook = PostgresHook(postgres_conn_id="postgres_conn")
    conn = postgres_hook.get_conn()
    cur = conn.cursor()

    # 쿼리 실행: 기존 테이블에서 데이터를 선택하여 새로운 테이블을 생성
    create_table_query = """
        BEGIN;

        DROP TABLE IF EXISTS temp;

        CREATE TABLE temp (
            movieCd VARCHAR(50) PRIMARY KEY,
            peopleNm VARCHAR(70),
            peopleNmEn VARCHAR(70)
        );

        -- 임시 테이블로 데이터 복사 (이 부분은 COPY 명령을 통해 데이터를 가져오는 방식이 필요합니다.)
        -- COPY temp (movieCd, peopleNm, peopleNmEn) FROM 'your_file.csv' WITH CSV HEADER DELIMITER ',' QUOTE '"';

        -- 잘못된 데이터 삭제
        DELETE FROM temp
        WHERE movieCd = 'movieCd' AND peopleNm = 'peopleNm';

        -- 새로운 director 테이블 생성
        DROP TABLE IF EXISTS director;

        CREATE TABLE director AS
        SELECT DISTINCT *
        FROM temp;

        COMMIT;

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
    "start_date": datetime(2024, 3, 7),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG 정의
dag = DAG(
    "postgres_director_table",
    default_args=default_args,
    description="A DAG to make director table in PSQL",
    schedule_interval="0 12 * * *",
)

# 작업 정의
t1 = PythonOperator(
    task_id="postgres_director_table",
    python_callable=manipulate_postgres_data,
    dag=dag,
)
