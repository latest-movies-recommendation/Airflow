import json
from datetime import datetime, timedelta
from io import StringIO
import logging

import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.athena import AthenaOperator


def yesterday_date_format():
    now = datetime.now() - timedelta(days=1)
    return now.strftime("%Y%m%d")


# test test
default_args = {
    "owner": "dongbin",
    "start_date": datetime(2024, 2, 26),
    "retries": 0,
    "retry_delay": timedelta(minutes=10),
}


@dag(
    dag_id="kofic_etl_v2",
    default_args=default_args,
    schedule_interval="0 1 * * *",
    catchup=False,
)
def kofic_etl_v2():

    @task
    def get_daily_box_office():
        api_key = Variable.get("kofic_key")

        # context = get_current_context()
        # execution_date = context["ds"]
        # target_date = datetime.strptime(execution_date, "%Y-%m-%d").strftime("%Y%m%d")
        target_date = yesterday_date_format()
        base_url = "http://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json"
        combinations = [
            ("", ""),
        ]

        rows = []
        for multiMovieYn, repNationCd in combinations:
            params = {
                "key": api_key,
                "targetDt": target_date,
                "multiMovieYn": multiMovieYn,
                "repNationCd": repNationCd,
            }
            response = requests.get(base_url, params=params)
            if response.status_code == 200:
                data = response.json()
                boxofficeType = data["boxOfficeResult"]["boxofficeType"]
                showRange = data["boxOfficeResult"]["showRange"].split("~")[0]
                for movie in data["boxOfficeResult"]["dailyBoxOfficeList"]:
                    row = [
                        boxofficeType,
                        showRange,
                        multiMovieYn,
                        repNationCd,
                        movie.get("rnum", ""),
                        movie.get("rank", ""),
                        movie.get("rankInten", ""),
                        movie.get("rankOldAndNew", ""),
                        movie.get("movieCd", ""),
                        movie.get("movieNm", ""),
                        movie.get("openDt", ""),
                        movie.get("salesAmt", ""),
                        movie.get("salesShare", ""),
                        movie.get("salesInten", ""),
                        movie.get("salesChange", ""),
                        movie.get("salesAcc", ""),
                        movie.get("audiCnt", ""),
                        movie.get("audiInten", ""),
                        movie.get("audiChange", ""),
                        movie.get("audiAcc", ""),
                        movie.get("scrnCnt", ""),
                        movie.get("showCnt", ""),
                    ]
                    rows.append(row)

        columns = [
            "boxofficeType",
            "showRange",
            "multiMovieYn",
            "repNationCd",
            "rnum",
            "rank",
            "rankInten",
            "rankOldAndNew",
            "movieCd",
            "movieNm",
            "openDt",
            "salesAmt",
            "salesShare",
            "salesInten",
            "salesChange",
            "salesAcc",
            "audiCnt",
            "audiInten",
            "audiChange",
            "audiAcc",
            "scrnCnt",
            "showCnt",
        ]
        df = pd.DataFrame(rows, columns=columns)

        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_data = csv_buffer.getvalue()

        bucket_name = Variable.get("s3_bucket_name")
        s3_hook = S3Hook(aws_conn_id="aws_conn")
        file_key = f"kofic/daily-box-office/{target_date}.csv"
        s3_hook.load_string(
            string_data=csv_data,
            key=file_key,
            bucket_name=bucket_name,
            replace=True,
        )
        # 로그 메시지 추가
        logging.info(f"File uploaded to S3: {bucket_name}/{file_key}")
        movie_cds = [
            movie.get("movieCd")
            for movie in data["boxOfficeResult"]["dailyBoxOfficeList"]
        ]
        return movie_cds

    @task
    def get_movie(movie_cds):
        api_key = Variable.get("kofic_key")

        # context = get_current_context()
        # execution_date = context["ds"]
        # target_date = datetime.strptime(execution_date, "%Y-%m-%d").strftime("%Y%m%d")
        target_date = yesterday_date_format()

        for movie_cd in movie_cds:
            response = requests.get(
                "http://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieInfo.json",
                params={"key": api_key, "movieCd": movie_cd},
            )

            movie_info = response.json()["movieInfoResult"]["movieInfo"]

            print(movie_info)

            bucket_name = Variable.get("s3_bucket_name")
            s3_hook = S3Hook(aws_conn_id="aws_conn")
            s3_hook.load_string(
                string_data=json.dumps(movie_info, ensure_ascii=False),
                key=f"kofic/movie2/{target_date}/{movie_cd}.json",
                bucket_name=bucket_name,
                replace=True,
            )

    # Trigger naver_crawler DAG . . ddd
    trigger_naver_crawler = TriggerDagRunOperator(
        task_id="trigger_naver_crawler",
        trigger_dag_id="naver_crawler",  # Make sure this matches the dag_id of the naver_crawler DAG
    )

    # Trigger watcha_comments DAG
    trigger_watcha_comments = TriggerDagRunOperator(
        task_id="trigger_watcha_comments",
        trigger_dag_id="watcha_comments",  # Make sure this matches the dag_id of the watcha_comments DAG
    )

    # Trigger daily_box_office_rds DAG
    trigger_daily_box_office_rds = TriggerDagRunOperator(
        task_id="trigger_daily_box_office_rds",
        trigger_dag_id="daily_box_office_rds",  # Make sure this matches the dag_id of the watcha_comments DAG
    )
    # 영화 코드 목록을 get_daily_box_office에서 받아 get_movie로 전달
    movie_cds_result = get_daily_box_office()
    get_movie(movie_cds=movie_cds_result)

    movie_cds_result >> trigger_naver_crawler
    movie_cds_result >> trigger_watcha_comments
    movie_cds_result >> trigger_daily_box_office_rds


kofic_etl_v2()
