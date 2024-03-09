import logging
import time
from datetime import datetime, timedelta
from io import StringIO

import pandas as pd
from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver import ActionChains
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys


# s3에서 파일 리스트 불러오기 =>리뷰 파일/영화 정보 파일이 있는지 확인 위함
def read_s3_filelist():
    s3 = S3Hook(aws_conn_id="aws_conn")
    bucket_name = Variable.get("s3_bucket_name")

    if s3 is not None:
        logging.info("S3에 성공적으로 연결되었습니다.")
    else:
        logging.error("S3 연결에 실패했습니다.")
        return None

    try:
        response = s3.list_keys(bucket_name, prefix="naver/")
        file_list = response if response else []
        return file_list
    except Exception as e:
        logging.error(f"S3에서 파일 목록을 가져오는 중 오류 발생: {e}")
        return None


# s3에 파일 업로드
def upload_to_s3(dataframe, file_name):
    s3 = S3Hook(aws_conn_id="aws_conn")
    try:
        csv_buffer = StringIO()
        dataframe.to_csv(csv_buffer, index=False)

        s3.load_string(
            string_data=csv_buffer.getvalue(),
            key=file_name,
            bucket_name=Variable.get("s3_bucket_name"),
            replace=True,
        )
        logging.info(f"파일 {file_name}이(가) 성공적으로 S3에 업로드되었습니다.")
    except Exception as e:
        logging.error(f"S3에 파일을 업로드하는 중 오류 발생: {e}")


# s3에 이미 파일이 존재할 경우 추가하기
def update_s3_file(dataframe, s3_key):
    # s3 파일 읽기
    s3 = S3Hook(aws_conn_id="aws_conn")
    try:
        obj = s3.get_key(key=s3_key, bucket_name=Variable.get("s3_bucket_name"))
        if obj:
            csv_data = obj.get()["Body"].read().decode("utf-8")
            s3_dataframe = pd.read_csv(StringIO(csv_data))

    except Exception as e:
        logging.info(f"Failed to read CSV file from S3: {e}")
        return

    try:
        merged_df = pd.concat([s3_dataframe, dataframe]).drop_duplicates(keep=False)
        csv_buffer = StringIO()
        merged_df.to_csv(csv_buffer, index=False)
        s3.load_string(
            string_data=csv_buffer.getvalue(),
            key=s3_key,
            bucket_name=Variable.get("s3_bucket_name"),
            replace=True,
        )
        logging.info("데이터가 성공적으로 추가되었습니다.")
    except Exception as e:
        logging.error(f"S3에 데이터를 추가하는 데 실패했습니다: {e}")


def s3_to_300movie_list():
    try:
        s3 = S3Hook(aws_conn_id="aws_conn")
        obj = s3.get_key(
            key="naver/알바영화리스트.csv",
            bucket_name=Variable.get("s3_bucket_name"),
        )
        if obj:
            csv_data = obj.get()["Body"].read().decode("utf-8")
            df = pd.read_csv(StringIO(csv_data))

            if "MOVIENM" not in df.columns:
                raise ValueError("MOVIENM 컬럼이 데이터프레임에 존재하지 않습니다.")
            # 영진위 1-10위 영화
            movies = df["MOVIENM"].tolist()[:100]
            logging.info(movies)
            return movies
        else:
            return None
    except Exception as e:
        logging.info(f"오류 발생: {e}")
        return []


def naver_review_crawling(**kwargs):

    movie_nm = []
    naver_reviews = []
    naver_review_id = []
    naver_review_date = []
    naver_review_time = []
    naver_review_score = []

    file_list = read_s3_filelist()
    movies = kwargs["ti"].xcom_pull(task_ids="s3_to_300movie_list")

    options = Options()
    options.add_argument("--headless")
    options.add_argument("window-size=1200x600")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=options)

    driver.get("https://www.naver.com")
    driver.implicitly_wait(2)

    for movie in movies:
        movie_review_search = f"영화 {movie} 관람평"
        # 1. 영화 검색
        movie_input = driver.find_element(By.ID, "query")
        ActionChains(driver).send_keys_to_element(
            movie_input, movie_review_search
        ).perform()
        time.sleep(1)
        movie_input.send_keys(Keys.RETURN)
        driver.implicitly_wait(2)
        try:
            driver.find_element(By.CLASS_NAME, "cm_pure_box.lego_rating_slide_outer")

            review_list = driver.find_element(
                By.CLASS_NAME, "lego_review_list._scroller"
            )
            before_h = driver.execute_script(
                "return arguments[0].scrollHeight;", review_list
            )

            while True:
                # 맨 아래로 스크롤을 내린다.
                driver.execute_script(
                    "arguments[0].scrollBy(0, arguments[0].scrollHeight);", review_list
                )
                # 스크롤하는 동안의 페이지 로딩 시간
                time.sleep(1)

                # 스크롤 후 높이
                after_h = driver.execute_script(
                    "return arguments[0].scrollHeight;", review_list
                )
                if after_h == before_h:
                    break
                before_h = after_h

            review_list = review_list.find_element(
                By.CLASS_NAME, "area_card_outer._item_wrapper"
            )
            reviews = review_list.find_elements(By.CLASS_NAME, "area_card._item")

            for review in reviews:
                review_content = review.get_attribute("data-report-title")

                if review_content != " ":
                    review_date = review.get_attribute("data-report-time")[
                        :8
                    ]  # 작성 날짜
                    review_time = review.get_attribute("data-report-time")[
                        -5:
                    ]  # 작성 시간
                    review_id = review.get_attribute(
                        "data-report-writer-id"
                    )  # 작성 아이디
                    review_score = (
                        review.find_element(By.CLASS_NAME, "area_text_box")
                        .text[12:]
                        .replace("\n", "")
                    )

                    # 리뷰 파일
                    if review_score == "10" or review_score == "1":
                        movie_nm.append(movie)
                        naver_review_id.append(review_id)
                        naver_review_date.append(review_date)
                        naver_review_time.append(review_time)
                        naver_reviews.append(review_content)
                        naver_review_score.append(review_score)

        except NoSuchElementException:
            pass

        driver.get("https://www.naver.com")

    # 영화 리뷰 dataframe
    naver_movie_reviews = pd.DataFrame(
        {
            "movie": movie_nm,
            "id": naver_review_id,
            "naver_review": naver_reviews,
            "review_date": naver_review_date,
            "review_date_time": naver_review_time,
            "review_score": naver_review_score,
        }
    )

    if "naver/naver_300reviews.csv" in file_list:
        update_s3_file(naver_movie_reviews, "naver/naver_300reviews.csv")
    else:
        upload_to_s3(naver_movie_reviews, "naver/naver_300reviews.csv")


default_args = {
    "start_date": datetime(2024, 2, 18),
    "retries": 1,
    "retry_delay": timedelta(minutes=20),
}

with DAG(
    dag_id="z_300movies_review_crawler",
    schedule_interval="@monthly",  # 평일 실행
    catchup=False,
    default_args=default_args,
) as dag:

    s3_to_300movie_list = PythonOperator(
        task_id="s3_to_300movie_list", python_callable=s3_to_300movie_list, dag=dag
    )
    naver_review_crawling = PythonOperator(
        task_id="naver_review_crawling", python_callable=naver_review_crawling, dag=dag
    )

    s3_to_300movie_list >> naver_review_crawling
