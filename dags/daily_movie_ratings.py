import json
import logging
from datetime import datetime, timedelta
from io import StringIO

import numpy as np
import pandas as pd
import sqlalchemy
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from sqlalchemy import create_engine

default_args = {
    "owner": "eunji",
    "start_date": datetime(2024, 2, 28),
    "retries": 0,
    "retry_delay": timedelta(minutes=10),
}


@dag(
    dag_id="daily_movie_ratings_db_3",
    default_args=default_args,
    schedule_interval="10 1 * * *",
    catchup=False,
)
def daily_movie_ratings_dag():
    @task
    def get_naver_ratings():
        s3_hook = S3Hook(aws_conn_id="aws_conn")
        s3_key = "naver/naver_movie_score.csv"
        try:
            obj = s3_hook.get_key(
                key=s3_key, bucket_name=Variable.get("s3_bucket_name")
            )
            if obj:
                # CSV 파일 데이터를 데이터프레임으로 읽어온 후 전처리
                csv_data = obj.get()["Body"].read().decode("utf-8")
                naver_rating = pd.read_csv(StringIO(csv_data))
                naver_rating.rename(
                    columns={
                        "movieNm": "movie_name",
                        "movieCd": "movie_code",
                        "entire_grade": "naver_rating",
                        "male_grade": "naver_male",
                        "female_grade": "naver_female",
                        "critic_grade": "naver_critics",
                    },
                    inplace=True,
                )
                naver_rating = naver_rating.replace("평점 없음", np.nan)
                logging.info(naver_rating)
                logging.info(f"{s3_key} 파일 다운로드 및 데이터프레임으로의 변환 성공!")
                return naver_rating.to_json(orient="split")
            else:
                logging.info(f"S3에 {s3_key} 파일이 없습니다.")
                return None
        except Exception as e:
            logging.error(f"S3로부터 데이터를 로드하는 데 실패했습니다.: {e}")
            return None

    @task
    def get_watcha_ratings(naver_ratings_json):
        if naver_ratings_json is None:
            logging.info("NAVER 평점 데이터가 없으므로 Watcha 평점 수집을 건너뜁니다.")
            return None
        naver_ratings = pd.read_json(naver_ratings_json, orient="split")
        codes = naver_ratings["movie_code"].tolist()

        s3_hook = S3Hook(aws_conn_id="aws_conn")
        bucket_name = Variable.get("s3_bucket_name")
        watcha_ratings = {}

        for code in codes:
            # 제목을 파일 이름으로 변환하여 S3 버킷에서 파일 찾기
            file_key = f"watcha/movies/m{code}.csv"
            # S3 버킷에서 파일 존재 여부 확인
            if s3_hook.check_for_key(file_key, bucket_name):
                # 파일이 존재하는 경우, 가져오기
                obj = s3_hook.get_key(key=file_key, bucket_name=bucket_name)
                csv_data = obj.get()["Body"].read().decode("utf-8")
                score_df = pd.read_csv(StringIO(csv_data)).iloc[:, 4]
                numeric_df = pd.to_numeric(score_df, errors="coerce")
                numeric_df.dropna(inplace=True)
                average_rating = numeric_df.mean()
                # 10점 만점이 되도록 평점 조정 후 딕셔너리에 저장
                watcha_ratings[code] = average_rating * 2
                logging.info(watcha_ratings)
            else:
                print(f"No CSV file found for m{code}")
        return json.dumps(watcha_ratings)

    @task
    def merge_ratings(naver_ratings_json, watcha_ratings_json):
        if naver_ratings_json is None or watcha_ratings_json is None:
            logging.info("필요한 평점 데이터가 없어 병합을 수행할 수 없습니다.")
            return None
        naver_ratings = pd.read_json(naver_ratings_json, orient="split")
        watcha_ratings = json.loads(watcha_ratings_json)

        naver_ratings["movie_code"] = naver_ratings["movie_code"].astype(str)

        watcha_rating_df = pd.DataFrame(
            list(watcha_ratings.items()), columns=["movie_code", "watcha_rating"]
        )
        watcha_rating_df["movie_code"] = watcha_rating_df["movie_code"].astype(str)

        merged_df = pd.merge(
            naver_ratings, watcha_rating_df, on="movie_code", how="left"
        )
        logging.info(merged_df)
        # CSV 파일로 저장
        merged_csv_path = "/tmp/merged_movie_ratings.csv"
        merged_df.to_csv(merged_csv_path, index=False)

        # 저장된 파일 경로를 반환
        return merged_csv_path

    @task
    def upload_to_s3(merged_csv_path):
        if merged_csv_path is None:
            logging.info("병합된 평점 데이터 파일이 제공되지 않았습니다.")
            return
        context = get_current_context()
        execution_date = context["ds"]
        date = datetime.strptime(execution_date, "%Y-%m-%d").strftime("%Y%m%d")
        file_name = f"{date}_rating.csv"
        key = f"daily-movie-ratings/{file_name}"
        merged_df = pd.read_csv(merged_csv_path)
        s3_hook = S3Hook(aws_conn_id="aws_conn")
        s3_bucket_name = Variable.get("s3_bucket_name")
        csv_buffer = StringIO()
        merged_df.to_csv(csv_buffer, index=False)
        csv_string = csv_buffer.getvalue()
        s3_hook.load_string(
            string_data=csv_string,
            key=key,
            bucket_name=s3_bucket_name,
            replace=True,
        )
        logging.info(f"{file_name} S3에 신규 업로드 완료!")

    @task
    def upload_to_postgres(merged_csv_path):
        if merged_csv_path is None:
            logging.info("병합된 평점 데이터 파일이 제공되지 않았습니다.")
            return

        # CSV 파일에서 DataFrame 로드
        merged_df = pd.read_csv(merged_csv_path)

        target_columns = [
            "watcha_rating",
            "naver_rating",
            "naver_male",
            "naver_female",
            "naver_critics",
        ]
        merged_df[target_columns] = merged_df[target_columns].astype(float)

        postgres_user = Variable.get("postgres_user")
        postgres_pwd = Variable.get("postgres_pwd")
        postgres_endpoint = Variable.get("postgres_endpoint")
        engine = create_engine(
            f"postgresql+psycopg2://{postgres_user}:{postgres_pwd}@{postgres_endpoint}:5432/postgres"
        )

        # DataFrame을 SQL 테이블에 삽입
        logging.info(merged_df.head())

        merged_df.to_sql(
            "daily_movie_ratings",
            engine,
            if_exists="replace",
            index=False,
            method="multi",
            dtype={
                "movie_name": sqlalchemy.types.VARCHAR(),
                "movie_code": sqlalchemy.types.VARCHAR(),
                "naver_rating": sqlalchemy.types.Float(precision=3),
                "watcha_rating": sqlalchemy.types.Float(precision=3),
                "naver_male": sqlalchemy.types.Float(precision=3),
                "naver_female": sqlalchemy.types.Float(precision=3),
                "naver_critics": sqlalchemy.types.Float(precision=3),
            },
        )

    # 태스크 정의
    naver_ratings_json = get_naver_ratings()
    watcha_ratings_json = get_watcha_ratings(naver_ratings_json)
    merged_csv_path = merge_ratings(naver_ratings_json, watcha_ratings_json)

    # 병렬 실행을 위한 플로우 설정
    upload_to_s3_task = upload_to_s3(merged_csv_path)
    upload_to_postgres_task = upload_to_postgres(merged_csv_path)

    # 의존성 설정
    merged_csv_path >> [upload_to_s3_task, upload_to_postgres_task]


dag_instance = daily_movie_ratings_dag()
