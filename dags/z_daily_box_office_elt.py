import json
import logging
from datetime import datetime, timedelta
from io import StringIO

import pandas as pd
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine


def yesterday_date_format():
    now = datetime.now() - timedelta(days=1)
    return now.strftime("%Y%m%d")


default_args = {
    "owner": "yein",
    "start_date": datetime(2003, 11, 11),
    "retries": 0,
    "retry_delay": timedelta(minutes=10),
}


@dag(
    dag_id="daily_box_office_elt",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    catchup=False,
)
def daily_box_office_elt():
    @task
    def daily_box_office_s3_to_postgres():
        s3_hook = S3Hook(aws_conn_id="aws_conn")
        # context = get_current_context()
        # execution_date = context["ds"]
        # target_date = datetime.strptime(execution_date, "%Y-%m-%d").strftime("%Y%m%d")
        target_date = yesterday_date_format()
        bucket_name = Variable.get("s3_bucket_name")
        object_key = f"kofic/daily-box-office/{target_date}.csv"
        csv_content = s3_hook.read_key(object_key, bucket_name)

        df = pd.read_csv(StringIO(csv_content))

        column_mapping = {
            "showRange": "box_office_date",
            "multiMovieYn": "nation_code",
            "repNationCd": "movie_type_code",
            "rankInten": "rank_intensity",
            "movieCd": "movie_code",
            "salesAmt": "sales_amount",
            "salesShare": "sales_share",
            "salesInten": "sales_intensity",
            "salesChange": "sales_change",
            "salesAcc": "sales_accumulated",
            "audiCnt": "audience_count",
            "audiInten": "audience_intensity",
            "audiChange": "audience_change",
            "audiAcc": "audience_accumulated",
            "scrnCnt": "screen_count",
            "showCnt": "show_count",
        }
        df.rename(columns=column_mapping, inplace=True)

        df["box_office_date"] = pd.to_datetime(
            df["box_office_date"], format="%Y%m%d"
        ).dt.strftime("%Y-%m-%d")
        df["nation_code"] = df["nation_code"].apply(lambda x: 0 if x == "K" else 1)
        df["movie_type_code"] = df["movie_type_code"].apply(
            lambda x: 1 if x == "Y" else 0
        )

        selected_columns = [
            "box_office_date",
            "nation_code",
            "movie_type_code",
            "rank",
            "rank_intensity",
            "movie_code",
            "sales_amount",
            "sales_share",
            "sales_intensity",
            "sales_change",
            "sales_accumulated",
            "audience_count",
            "audience_intensity",
            "audience_change",
            "audience_accumulated",
            "screen_count",
            "show_count",
        ]
        df = df[selected_columns]

        postgres_hook = PostgresHook(postgres_conn_id="postgres_conn")
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()

        engine = create_engine(postgres_hook.get_uri(), echo=False)
        df.to_sql(
            "box_office", con=engine, if_exists="append", index=False, method="multi"
        )

        cursor.close()
        conn.close()

    @task
    def movie_s3_to_postgres():
        s3_hook = S3Hook(aws_conn_id="aws_conn")
        bucket_name = Variable.get("s3_bucket_name")

        # context = get_current_context()
        # execution_date = context["ds"]
        # target_date = datetime.strptime(execution_date, "%Y-%m-%d").strftime("%Y%m%d")
        target_date = yesterday_date_format()

        prefix = f"kofic/movies-to-fetch/{target_date}/"

        keys = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix)
        if not keys:
            raise ValueError(f"No files found for {target_date}")

        movie_cds = []
        for key in keys:
            if key.split(".")[-1] == "csv":
                content = s3_hook.read_key(key=key, bucket_name=bucket_name)
                df = pd.read_csv(StringIO(content))
                movie_cds.extend(df["moviecd"].tolist())

        combined_df = pd.DataFrame()
        for movie_cd in movie_cds:
            try:
                key = f"kofic/movie/{target_date}/{movie_cd}.json"
                json_content = s3_hook.read_key(key=key, bucket_name=bucket_name)
                json_data = json.loads(json_content)
                df = pd.json_normalize(json_data)
                logging.info(df)
                combined_df = pd.concat([combined_df, df], ignore_index=True)
            except Exception as e:
                logging.info(f"{movie_cd}: {e}")

        column_mapping = {
            "movieCd": "code",
            "movieNm": "korean_name",
            "movieNmEn": "english_name",
            "movieNmOg": "original_name",
            "showTm": "show_time",
            "prdtYear": "production_year",
            "openDt": "open_date",
        }
        combined_df.rename(columns=column_mapping, inplace=True)

        # 정수형 데이터 변환 및 유효하지 않은 값 대체
        for column in ["show_time", "production_year"]:
            combined_df[column] = (
                pd.to_numeric(combined_df[column], errors="coerce")
                .fillna(0)
                .astype(int)
            )

        combined_df["open_date"] = pd.to_datetime(
            combined_df["open_date"], format="%Y%m%d", errors="coerce"
        )
        selected_columns = [
            "code",
            "korean_name",
            "english_name",
            "original_name",
            "show_time",
            "production_year",
            "open_date",
        ]
        combined_df = combined_df[selected_columns]
        postgres_hook = PostgresHook(postgres_conn_id="postgres_conn")
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()
        engine = create_engine(postgres_hook.get_uri(), echo=False)
        combined_df.to_sql(
            "movie", con=engine, if_exists="append", index=False, method="multi"
        )
        cursor.close()
        conn.close()

    movie = movie_s3_to_postgres()
    box_office = daily_box_office_s3_to_postgres()

    movie >> box_office


daily_box_office_elt()
