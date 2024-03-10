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
        -- 믿고보는 배우(한달에 한번실행)
        -- 테이블의 모든 행 삭제
        TRUNCATE TABLE djan_trusted_actor;
        -- 데이터 다시 삽입
        INSERT INTO djan_trusted_actor (actor, number, avg_audience, audience_showcnt)
        WITH tem AS (
            SELECT moviecd, movienm, MAX(CAST(AUDIACC AS INTEGER)) AS AUDIACC, SUM(CAST(showcnt AS INTEGER)) AS showcnt, MAX(CAST(AUDIACC AS INTEGER)) / SUM(CAST(showcnt AS INTEGER)) AS audience_showcnt
            FROM all_box_office
            GROUP BY moviecd, movienm
            HAVING MAX(CAST(AUDIACC AS INTEGER)) >= 1000000
        )
        SELECT
            A.peoplenm,
            COUNT(A.peoplenm) AS number,
            AVG(B.audiacc) AS avg_audience,
            AVG(B.audience_showcnt) AS audience_showcnt
        FROM
            tem AS B
        JOIN
            detail_actor AS A ON A.moviecd = B.moviecd
        WHERE
            CAST(A.actorpos AS INTEGER) <= 3
        GROUP BY
            A.peoplenm
        HAVING
            AVG(B.audience_showcnt) >= 10 AND COUNT(A.peoplenm) >= 3
        ORDER BY
            avg(B.audiacc) DESC;
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
    "psql_djan_trusted_actor",
    default_args=default_args,
    description="A simple DAG to manipulate PostgreSQL data",
    schedule_interval="0 0 1 * *",
)

# 작업 정의
t1 = PythonOperator(
    task_id="manipulate_data",
    python_callable=manipulate_postgres_data,
    dag=dag,
)
