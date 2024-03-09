import csv
import json
import logging
from datetime import datetime, timedelta
from io import StringIO

import psycopg2
import psycopg2.extras
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook


def fetch_and_upload_naver_trends():
    # API 설정
    client_id = Variable.get("naver_client_id")
    client_secret = Variable.get("naver_client_secret")
    url = "https://openapi.naver.com/v1/datalab/search"
    headers = {
        "X-Naver-Client-Id": client_id,
        "X-Naver-Client-Secret": client_secret,
        "Content-Type": "application/json",
    }
    # 검색어 리스트와 날짜 설정 .
    keywords = ["파묘", "듄2", "건국전쟁", "패스트 라이브즈", "웡카"]
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    body = {
        "startDate": start_date.strftime("%Y-%m-%d"),
        "endDate": end_date.strftime("%Y-%m-%d"),
        "timeUnit": "date",
        "keywordGroups": [
            {"groupName": keyword, "keywords": [keyword]} for keyword in keywords
        ],
    }

    # API 요청 및 응답
    response = requests.post(url, headers=headers, data=json.dumps(body))
    data = response.json()

    # 상태 코드 확인
    if response.status_code == 200:
        logging.info("Request successful.")
        # 필요하다면, 응답 데이터에 대한 추가 정보를 로깅할 수 있습니다.
        logging.info("Response data: %s", data)
    else:
        # 요청 실패 시, 상태 코드와 함께 실패 로그를 남깁니다.
        logging.error("Request failed with status code: %s", response.status_code)
        logging.error("Error response data: %s", data)

    # CSV 데이터 생성
    output = StringIO()
    csv_writer = csv.writer(output)
    csv_writer.writerow(["Date", "Keyword", "Ratio"])
    for result in data["results"]:
        for data_point in result["data"]:
            csv_writer.writerow(
                [data_point["period"], result["title"], data_point["ratio"]]
            )

    # S3 업로드
    output.seek(0)
    s3_hook = S3Hook(aws_conn_id="aws_conn")
    s3_bucket_name = Variable.get("s3_bucket_name")
    s3_file_path = f'naver/naver_trend/{end_date.strftime("%Y-%m-%d")}.csv'
    s3_hook.load_string(
        output.getvalue(), s3_file_path, bucket_name=s3_bucket_name, replace=True
    )

    return s3_file_path


def upload_to_rds(**kwargs):
    ti = kwargs["ti"]
    s3_file_path = ti.xcom_pull(task_ids="fetch_and_upload_naver_trends")

    # S3에서 CSV 데이터 읽기
    s3_hook = S3Hook(aws_conn_id="aws_conn")
    s3_bucket_name = Variable.get("s3_bucket_name")
    csv_data = s3_hook.read_key(s3_file_path, bucket_name=s3_bucket_name)

    # RDS에 데이터 삽입
    pg_hook = PostgresHook(postgres_conn_id="postgres_conn")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS naver_trend (
            date DATE NOT NULL,
            keyword VARCHAR(255) NOT NULL,
            ratio FLOAT NOT NULL
        );
    """
    )
    csv_reader = csv.reader(StringIO(csv_data))
    next(csv_reader)  # Skip header
    psycopg2.extras.execute_batch(
        cursor,
        """
    INSERT INTO naver_trend (date, keyword, ratio) VALUES (%s, %s, %s)
    """,
        list(csv_reader),
    )
    connection.commit()


dag = DAG(
    dag_id="naver_trend_upload_to_s3_and_rds",
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "start_date": datetime(2024, 3, 9),
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="Fetch Naver trends, upload to S3 and insert into RDS daily",
    schedule_interval=timedelta(days=1),
    catchup=False,
)

t1 = PythonOperator(
    task_id="fetch_and_upload_naver_trends",
    python_callable=fetch_and_upload_naver_trends,
    dag=dag,
)

t2 = PythonOperator(
    task_id="upload_to_rds",
    python_callable=upload_to_rds,
    provide_context=True,
    dag=dag,
)

t1 >> t2
