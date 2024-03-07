import json
from datetime import datetime, timedelta
from io import StringIO

import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.athena import AthenaOperator


def yesterday_date_format():
    now = datetime.now() - timedelta(days=1)
    return now.strftime("%Y%m%d")


# test test
default_args = {
    "owner": "yein",
    "start_date": datetime(2003, 11, 11),
    "retries": 0,
    "retry_delay": timedelta(minutes=10),
}


@dag(
    dag_id="z_kofic_etl",
    default_args=default_args,
    schedule_interval="0 1 * * *",
    catchup=False,
)
def kofic_etl():

    @task
    def get_daily_box_office():
        api_key = Variable.get("kofic_key")

        # context = get_current_context()
        # execution_date = context["ds"]
        # target_date = datetime.strptime(execution_date, "%Y-%m-%d").strftime("%Y%m%d")
        target_date = yesterday_date_format()
        base_url = "http://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json"
        combinations = [
            ("Y", "K"),
            ("Y", "F"),
            ("N", "K"),
            ("N", "F"),
            ("Y", ""),
            ("N", ""),
            ("", "K"),
            ("", "F"),
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
        s3_hook.load_string(
            string_data=csv_data,
            key=f"kofic/daily-box-office/{target_date}.csv",
            bucket_name=bucket_name,
            replace=True,
        )

    database_name = Variable.get("athena_database_name")

    run_athena_query = AthenaOperator(
        task_id="store_movie_codes_to_fetch",
        query=f"""
            SELECT DISTINCT(dbo.moviecd), dbo.movienm
            FROM {database_name}.daily_box_office dbo
            LEFT JOIN {database_name}.movie2 m ON dbo.moviecd = m.moviecd
            WHERE m.moviecd IS NULL
                AND dbo.moviecd IS NOT NULL;
            """,
        database=database_name,
        output_location=f's3://{Variable.get("s3_bucket_name")}/kofic/movies-to-fetch/'
        + yesterday_date_format(),
        aws_conn_id="aws_conn",
    )

    @task
    def load_movie_codes_to_fetch():
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
                content = s3_hook.read_key(key, bucket_name=bucket_name)
                df = pd.read_csv(StringIO(content))
                movie_cds.extend(df["moviecd"].tolist())

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

    daily_box_office_data = get_daily_box_office()
    movie_cds_result = load_movie_codes_to_fetch()
    get_movie(movie_cds=movie_cds_result)

    daily_box_office_data >> run_athena_query
    run_athena_query >> movie_cds_result


kofic_etl()
