import logging
import re
from datetime import datetime, timedelta
from io import StringIO

import pandas as pd
from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
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


default_args = {
    "owner": "yein",
    "start_date": datetime(2003, 11, 11),
    "retries": 0,
    "retry_delay": timedelta(minutes=10),
}


@dag(
    dag_id="kofic_etl",
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
            LEFT JOIN {database_name}.movie m ON dbo.moviecd = m.moviecd
            WHERE m.moviecd IS NULL
                AND dbo.moviecd IS NOT NULL;
            """,
        database=database_name,
        output_location=f's3://{Variable.get("s3_bucket_name")}/kofic/movies-to-fetch/'
        + "{{ target_date_nodash }}",
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
                key=f"kofic/movie/{target_date}/{movie_cd}.json",
                bucket_name=bucket_name,
                replace=True,
            )

    daily_box_office_data = get_daily_box_office()
    movie_cds_result = load_movie_codes_to_fetch()
    get_movie(movie_cds=movie_cds_result)

    daily_box_office_data >> run_athena_query
    run_athena_query >> movie_cds_result


kofic_etl()


# 어제 날짜 ex)20240215 형식으로 리턴
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
            ti.xcom_push(key="movies_title", value=movies)
            logging.info(movies)
            return movies
        else:
            logging.info(f"S3에 {yesterday_date()}자 해당 파일이 없습니다.")
            return None
    except Exception as e:
        logging.info(f"S3로부터 데이터를 로드하는 데 실패했습니다.: {e}")


def setting_driver():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("window-size=1200x600")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=options)
    return driver


def scraping_watcha(**kwargs):
    ti = kwargs["ti"]
    titles = ti.xcom_pull(task_ids="get_daily_box_office", key="movies_title")
    # titles = ["파이트클럽", "무간도2", "사도", "펄프픽션", "테넷", "식스센스", "바빌론"]
    logging.info(titles)

    driver = setting_driver()
    if titles is not None:
        for title in titles:
            logging.info(f"{title} 리뷰 추출을 시작합니다.")
            try:
                url = f"https://pedia.watcha.com/ko-KR/search?query={title}"
                driver.get(url)
                driver.implicitly_wait(2)
                logging.info(f"{title}으로 Watcha에 접근중")

                a = driver.find_element(By.CLASS_NAME, "e1ic68ft4")
                link = a.get_attribute("href") + "/comments?order=recent"
                driver.get(link)
                driver.implicitly_wait(2)
                logging.info(f"{title}의 스크롤링을 시작합니다.")

                page_scrolling(driver)
                driver.implicitly_wait(10)
                logging.info(f"{title}의 스크롤링을 마쳤습니다.")

                reviews = driver.find_elements(By.CLASS_NAME, "egj9y8a4")
                data = []
                # 상위 200개의 리뷰만 가져오기
                for index, review in enumerate(reviews):
                    if index >= 200:
                        break

                    id = review.find_element(By.CLASS_NAME, "eovgsd00").text
                    likes = review.find_element(By.TAG_NAME, "em").text
                    try:
                        score_element = review.find_element(By.CLASS_NAME, "egj9y8a0")
                        score = (
                            score_element.text
                            if score_element.text != "보고싶어요"
                            or score_element.text != "보는 중"
                            else None
                        )
                    except NoSuchElementException:
                        score = None
                    comment = (
                        review.find_element(By.CLASS_NAME, "e1hvy88212")
                        .text.replace("\n", " ")
                        .replace('"', "")
                    )
                    # 각 리뷰의 정보를 data 리스트에 추가합니다.
                    data.append([id, score, likes, comment])

                df = pd.DataFrame(data, columns=["id", "score", "likes", "comment"])

                # 영화 제목 컬럼 추가
                df["movie_name"] = title

                # 수집한 날짜 컬럼 추가
                collected_date = datetime.now().date()
                df["collected_date"] = collected_date

                # 컬럼 순서 재정렬
                df = df[
                    ["id", "movie_name", "collected_date", "score", "likes", "comment"]
                ]
                logging.info("------새로 수집된 댓글 데이터프레임------")
                logging.info(df)

                # S3에 업로드
                upload_to_s3(title, df)

            except Exception as e:
                logging.info(f"selenium 접근 실패: {e}")
    else:
        logging.info("리턴받은 제목 없음.")


def page_scrolling(driver, target_height=73122, max_attempts=300):
    cnt = 0
    try:
        while cnt < max_attempts:
            cnt += 1
            driver.execute_script("window.scrollTo(0, window.pageYOffset + 500);")
            driver.implicitly_wait(10)
            scroll_height = driver.execute_script("return document.body.scrollHeight")
            if scroll_height >= target_height:
                break
            logging.info(f"스크롤링 {cnt}회")
    except Exception as e:
        logging.info(f"스크롤링 실패: {e}")
    finally:
        if cnt >= max_attempts:
            logging.info("스크롤링 시도 횟수 초과. 최대 시도 횟수에 도달했습니다.")


# DataFrame을 CSV 문자열로 변환
def get_csv_string(df):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_string = csv_buffer.getvalue()
    return csv_string


def upload_to_s3(title, df):
    # 파일 제목에 들어가서는 안 되는 문자 제거
    safe_title = re.sub(r'[\\/*?:"<>|]', "", title)
    file_name = f"{safe_title}.csv"
    key = f"watcha/movies/{file_name}"
    s3_hook = S3Hook(aws_conn_id="aws_conn")
    s3_bucket_name = Variable.get("s3_bucket_name")

    # 기존 파일이 있는지 확인
    try:
        # S3에서 기존 데이터프레임 로드
        obj = s3_hook.get_key(key=key, bucket_name=s3_bucket_name)
        if obj:
            existing_csv = obj.get()["Body"].read().decode("utf-8")
            existing_df = pd.read_csv(StringIO(existing_csv))
            logging.info(f"존재하는 파일이 있습니다. :{file_name}")

            # 기존 데이터프레임의 'collected_date' 열을 datetime 타입으로 변환
            existing_df["collected_date"] = pd.to_datetime(
                existing_df["collected_date"]
            )

            # 새 데이터프레임의 'collected_date' 열도 datetime 타입으로 변환
            df["collected_date"] = pd.to_datetime(df["collected_date"])

            # 새 데이터와 기존 데이터 병합 후 중복 제거
            combined_df = pd.concat([existing_df, df]).drop_duplicates(
                subset=["id"], keep="first", ignore_index=True
            )
            new_data = combined_df[~combined_df["id"].isin(existing_df["id"])]

            if not new_data.empty:
                # 중복되지 않는 새 데이터만 기존 파일에 추가
                final_df = pd.concat([existing_df, new_data], ignore_index=True)
                # 수집 일자 순으로 데이터 정렬
                final_df_sorted = final_df.sort_values(
                    by="collected_date", ascending=False
                )
                # S3에 업로드
                s3_hook.load_string(
                    string_data=get_csv_string(final_df_sorted),
                    key=key,
                    bucket_name=s3_bucket_name,
                    replace=True,
                )
                logging.info(
                    f"{file_name}에 새로운 데이터 추가 및 정렬하여 S3에 업로드 완료!"
                )

            else:
                logging.info("추가할 새로운 댓글이 없습니다.")

    except Exception as e:
        logging.info(
            f"기존 {file_name} 파일 처리 중 에러 발생 또는 파일 없음. 새로 업로드를 시도합니다.: {e}"
        )
        # 기존 파일이 없으면 새 파일로 업로드
        s3_hook.load_string(
            string_data=get_csv_string(df),
            key=key,
            bucket_name=s3_bucket_name,
            replace=True,
        )
        logging.info(f"{file_name} S3에 신규 업로드 완료!")


dag = DAG(
    dag_id="watcha_comments",
    start_date=datetime(2024, 2, 14),
    schedule="0 9 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "retry_delay": timedelta(minutes=3),
    },
)

get_daily_box_office = PythonOperator(
    task_id="get_daily_box_office",
    python_callable=get_daily_box_office,
    provide_context=True,
    dag=dag,
)

scraping_watcha = PythonOperator(
    task_id="scraping_watcha",
    python_callable=scraping_watcha,
    provide_context=True,
    dag=dag,
    execution_timeout=timedelta(minutes=30),  # 태스크의 최대 실행 시간을 15분으로 설정
)


kofic_etl >> get_daily_box_office >> scraping_watcha
