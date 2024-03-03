import json
import logging
import re
from datetime import datetime, timedelta
from io import StringIO

import pandas as pd
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def format_movie_titles(df):
    # 첫 번째 열의 제목들에 대해 포맷팅 적용
    df[df.columns[0]] = df[df.columns[0]].apply(
        lambda title: re.sub(r'[\\/*?:"<>|]', "", title)
    )
    return df


default_args = {
    "owner": "eunji",
    "start_date": datetime(2024, 2, 28),
    "retries": 0,
    "retry_delay": timedelta(minutes=10),
}


@dag(
    dag_id="daily_movie_ratings",
    default_args=default_args,
    schedule_interval="10 1 * * *",
    catchup=False,
)
def daily_movie_ratings():

    @task
    def get_naver_ratings(ti):
        s3_hook = S3Hook(aws_conn_id="aws_conn")
        s3_key = "naver/naver_reviews.csv"
        try:
            obj = s3_hook.get_key(
                key=s3_key, bucket_name=Variable.get("s3_bucket_name")
            )
            if obj:
                # CSV 파일 데이터를 Pandas DataFrame으로 읽어오기
                csv_data = obj.get()["Body"].read().decode("utf-8")
                naver_rating = pd.read_csv(StringIO(csv_data)).iloc[:, :2]
                # 영화 제목 형식 통일
                naver_rating_formatted = format_movie_titles(naver_rating)
                logging.info(f"{s3_key} 파일 다운로드 및 데이터프레임으로의 변환 성공!")
                ti.xcom_push(
                    key="naver_ratings",
                    value=naver_rating_formatted.to_json(orient="split"),
                )
                logging.info(naver_rating_formatted)
                movies = naver_rating_formatted.iloc[:, 0].tolist()
                ti.xcom_push(key="movies_title", value=movies)
                logging.info(movies)
                return movies
            else:
                logging.info(f"S3에 {s3_key} 파일이 없습니다.")
                return None
        except Exception as e:
            logging.info(f"S3로부터 데이터를 로드하는 데 실패했습니다.: {e}")

    @task
    def get_watcha_ratings(ti):
        titles = ti.xcom_pull(task_ids="get_naver_ratings", key="movies_title")
        s3_hook = S3Hook(aws_conn_id="aws_conn")
        bucket_name = Variable.get("s3_bucket_name")

        # 영화 제목과 평점을 매핑하는 딕셔너리
        watcha_ratings = {}
        for title in titles:
            # 제목을 파일 이름으로 변환하여 S3 버킷에서 파일 찾기
            file_key = f"watcha/movies/{title}.csv"
            # S3 버킷에서 파일 존재 여부 확인
            if s3_hook.check_for_key(file_key, bucket_name):
                # 파일이 존재하는 경우, 가져오기
                obj = s3_hook.get_key(key=file_key, bucket_name=bucket_name)
                csv_data = obj.get()["Body"].read().decode("utf-8")
                average_rating = pd.read_csv(StringIO(csv_data)).iloc[:, 3].mean()
                # 10점 만점이 되도록 평점 조정 후 딕셔너리에 저장
                watcha_ratings[title] = average_rating * 2
            else:
                print(f"No CSV file found for {title}")

        ti.xcom_push(key="watcha_ratings", value=json.dumps(watcha_ratings))

    @task
    def merge_ratings(ti):
        naver_ratings_json = ti.xcom_pull(
            task_ids="get_naver_ratings", key="naver_ratings"
        )
        naver_ratings = pd.read_json(naver_ratings_json, orient="split")

        watcha_ratings_json = ti.xcom_pull(
            task_ids="get_watcha_ratings", key="watcha_ratings"
        )
        watcha_ratings = json.loads(watcha_ratings_json)

        # `watcha_ratings` 딕셔너리를 DataFrame으로 변환
        watcha_rating_df = pd.DataFrame(
            list(watcha_ratings.items()), columns=["movie", "watcha_rating"]
        )

        # 두 DataFrame을 'movie' 컬럼을 기준으로 합치기
        merged_df = pd.merge(naver_ratings, watcha_rating_df, on="movie", how="left")
        logging.info(merged_df)

        return merged_df

    get_naver_ratings >> get_watcha_ratings >> merge_ratings


daily_movie_ratings()
