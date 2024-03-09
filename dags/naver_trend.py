import json
import logging
from datetime import datetime, timedelta
from io import BytesIO, StringIO

import matplotlib.font_manager as fm
import matplotlib.pyplot as plt
import pandas as pd
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def yesterday_date():
    now = datetime.now() - timedelta(days=1)
    return now.strftime("%Y%m%d")


def get_daily_box_office(ti):
    s3_hook = S3Hook(aws_conn_id="aws_conn")
    s3_key = f"kofic/daily-box-office/{yesterday_date()}.csv"
    try:
        obj = s3_hook.get_key(key=s3_key, bucket_name=Variable.get("s3_bucket_name"))
        if obj:
            # CSV 파일 데이터를 Pandas DataFrame으로 읽어오기
            csv_data = obj.get()["Body"].read().decode("utf-8")
            df = pd.read_csv(StringIO(csv_data))
            logging.info(
                f"{yesterday_date()}자 daily-box-office 파일 다운로드 및 데이터프레임으로의 변환 성공!"
            )
            if "movieNm" not in df.columns:
                raise ValueError("movieNm 컬럼이 데이터프레임에 존재하지 않습니다.")
            # 영진위 1-10위 영화
            movies = df["movieNm"].tolist()[-10:]
            movie_codes = df["movieCd"].tolist()[-10:]
            ti.xcom_push(key="movies_title", value=movies)
            ti.xcom_push(key="movies_code", value=movie_codes)
            logging.info(movies)
            logging.info(movie_codes)
            return movies
        else:
            logging.info(f"S3에 {yesterday_date()}자 해당 파일이 없습니다.")
            return None
    except Exception as e:
        logging.info(f"S3로부터 데이터를 로드하는 데 실패했습니다.: {e}")


def fetch_and_upload_naver_trends(**kwargs):
    ti = kwargs["ti"]
    keywords = ti.xcom_pull(task_ids="get_daily_box_office", key="movies_title")

    client_id = Variable.get("naver_client_id")
    client_secret = Variable.get("naver_client_secret")
    url = "https://openapi.naver.com/v1/datalab/search"
    headers = {
        "X-Naver-Client-Id": client_id,
        "X-Naver-Client-Secret": client_secret,
        "Content-Type": "application/json",
    }

    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)

    # 키워드 리스트를 5개씩 나누어 처리
    for i in range(0, len(keywords), 5):
        current_keywords = keywords[i : i + 5]

        body = {
            "startDate": start_date.strftime("%Y-%m-%d"),
            "endDate": end_date.strftime("%Y-%m-%d"),
            "timeUnit": "date",
            "keywordGroups": [
                {"groupName": keyword, "keywords": [keyword]}
                for keyword in current_keywords
            ],
        }

        response = requests.post(url, headers=headers, data=json.dumps(body))
        if response.status_code != 200:
            continue  # 오류 발생시 다음 그룹으로 넘어갑니다.

        data = response.json()
        records = []
        for group in data["results"]:
            for item in group["data"]:
                records.append(
                    {
                        "date": item["period"],
                        "keyword": group["title"],
                        "ratio": item["ratio"],
                    }
                )
        df = pd.DataFrame(records)
        font_path = "/usr/share/fonts/truetype/nanum/NanumBarunGothic.ttf"
        font_name = fm.FontProperties(fname=font_path).get_name()
        plt.rc("font", family=font_name)

        plt.figure(figsize=(10, 6))
        for keyword in current_keywords:
            df_keyword = df[df["keyword"] == keyword]
            plt.plot(
                pd.to_datetime(df_keyword["date"]),
                df_keyword["ratio"],
                label=keyword,
                marker="o",
            )
        plt.xlabel("Date")
        plt.ylabel("Ratio")
        plt.title(f"Daily Trends by Keyword: Group {i//5 + 1}")
        plt.legend()
        plt.xticks(rotation=45)
        plt.tight_layout()

        # 그래프를 이미지 파일로 변환
        img_buffer = BytesIO()
        plt.savefig(img_buffer, format="png")
        plt.close()
        img_buffer.seek(0)

        # S3 업로드
        s3_hook = S3Hook(aws_conn_id="aws_conn")
        s3_bucket_name = Variable.get("s3_bucket_name")
        s3_file_path = f"naver/naver_trend/trends_group_{i//5 + 1}.png"
        s3_hook.load_bytes(
            img_buffer.getvalue(),
            key=s3_file_path,
            bucket_name=s3_bucket_name,
            replace=True,
        )


dag = DAG(
    dag_id="naver_trend_upload_to_s3",
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

get_daily_box_office = PythonOperator(
    task_id="get_daily_box_office",
    python_callable=get_daily_box_office,
    dag=dag,
)

fetch_and_upload_naver_trends = PythonOperator(
    task_id="fetch_and_upload_naver_trends",
    python_callable=fetch_and_upload_naver_trends,
    dag=dag,
)

get_daily_box_office >> fetch_and_upload_naver_trends
