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


# 오늘 날짜 ex)20240216 형식으로 리턴
def today_date():
    now = datetime.now()
    return now.strftime("%Y%m%d")


def get_daily_box_office(ti, **kwargs):
    s3_hook = S3Hook(aws_conn_id="aws_conn")
    # s3_key = f"kofic/daily-box-office/{today_date()}.csv"
    s3_key = "kofic/daily-box-office/20240219.csv"
    try:
        obj = s3_hook.get_key(key=s3_key, bucket_name=Variable.get("s3_bucket_name"))
        if obj:
            # CSV 파일 데이터를 Pandas DataFrame으로 읽어오기
            csv_data = obj.get()["Body"].read().decode("utf-8")
            df = pd.read_csv(StringIO(csv_data))
            logging.info(
                f"{today_date()}자 daily-box-office 파일 다운로드 및 데이터프레임으로의 변환 성공!"
            )
            if "movieNm" not in df.columns:
                raise ValueError("movieNm 컬럼이 데이터프레임에 존재하지 않습니다.")
            # 영진위 1-10위 영화
            movies = df["movieNm"].tolist()[-10:]
            ti.xcom_push(key="movies_title", value=movies)
            logging.info(movies)
            return movies
        else:
            logging.info(f"S3에 {today_date()}자 해당 파일이 없습니다.")
            return None
    except Exception as e:
        logging.info(f"S3로부터 데이터를 로드하는 데 실패했습니다.: {e}")


def setting_driver():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("window-size=1200x600")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    # s = Service("/usr/bin/chromedriver")
    # driver = webdriver.Chrome(service=s, options=options)
    driver = webdriver.Chrome(options=options)
    return driver


def scraping_watcha(**kwargs):
    # ti = kwargs['ti']
    # titles = ti.xcom_pull(task_ids='get_daily_box_office', key='movies_title')
    titles = ["시시콜콜한 이야기", "애프터썬"]
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
                driver.implicitly_wait(2)
                logging.info(f"{title}스크롤링을 마쳤습니다.")

                reviews = driver.find_elements(By.CLASS_NAME, "egj9y8a4")
                data = []
                # 상위 200개의 리뷰만 가져오기
                for index, review in enumerate(reviews):
                    if index >= 200:
                        break

                    id = review.find_element(By.CLASS_NAME, "eovgsd00").text
                    like = review.find_element(By.TAG_NAME, "em").text
                    try:
                        score_element = review.find_element(By.CLASS_NAME, "egj9y8a0")
                        score = (
                            score_element.text
                            if score_element.text != "보고싶어요"
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
                    data.append([id, score, like, comment])

                df = pd.DataFrame(data, columns=["id", "score", "like", "comment"])

                # 수집한 날짜 컬럼 추가
                collected_date = datetime.now().date()
                df["collected_date"] = collected_date

                logging.info(df)

                # S3에 업로드
                upload_to_s3(title, df)

            except Exception as e:
                logging.info(f"selenium 접근 실패: {e}")
    else:
        logging.info("리턴받은 제목 없음.")


# 전체 페이지를 스크롤
# def page_scrolling(driver):
#     scroll_location = driver.execute_script("return document.body.scrollHeight")
#     cnt = 0
#     try:
#         while True:
#             cnt += 1
#             # 현재 스크롤의 가장 아래로 내림
#             driver.execute_script("window.scrollTo(0,document.body.scrollHeight)")
#
#             # 전체 스크롤이 늘어날 때까지 대기
#             driver.implicitly_wait(2)
#
#             # 늘어난 스크롤 높이
#             scroll_height = driver.execute_script("return document.body.scrollHeight")
#
#             # 늘어난 스크롤 위치와 이동 전 위치 같으면(더 이상 스크롤이 늘어나지 않으면) 종료
#             if scroll_location == scroll_height:
#                 break
#
#             # 같지 않으면 스크롤 위치 값을 수정하여 같아질 때까지 반복
#             else:
#                 # 스크롤 위치값을 수정
#                 scroll_location = driver.execute_script(
#                     "return document.body.scrollHeight"
#                 )
#             logging.info(f"스크롤링 {cnt}회")
#     except Exception as e:
#         logging.info(f"스크롤링 실패: {e}")


# 약 200개의 리뷰가 있는 위치까지 스크롤링
def page_scrolling(driver, target_height=73122):
    cnt = 0
    try:
        while True:
            cnt += 1
            driver.execute_script("window.scrollTo(0, window.pageYOffset + 500);")
            driver.implicitly_wait(2)
            scroll_height = driver.execute_script("return document.body.scrollHeight")
            if scroll_height >= target_height:
                break
            logging.info(f"스크롤링 {cnt}회")
    except Exception as e:
        logging.info(f"스크롤링 실패: {e}")


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
    logging.info(df)
    key = f"watcha/{file_name}"
    s3_hook = S3Hook(aws_conn_id="aws_conn")

    # 기존 파일이 있는지 확인
    try:
        existing_df = pd.read_csv(f"s3://{Variable.get('s3_bucket_name')}/{key}")
        logging.info(f"존재하는 파일이 있습니다. :{file_name}")

        merged_df = pd.concat([existing_df, df], ignore_index=True)
        # 중복되지 않는 데이터만 선택
        new_data = merged_df.drop_duplicates(keep=False)

        # S3에 업로드
        logging.info(f"Uploading {file_name} to s3")
        s3_hook.load_string(
            string_data=get_csv_string(new_data),
            key=key,
            bucket_name=Variable.get("s3_bucket_name"),
            replace=True,
        )  # 동일한 키가 있을 경우 덮어쓰기 설정
        logging.info(f"{file_name} S3에 업로드 완료!")

    except Exception as e:
        # 기존 파일이 없는 경우
        logging.info(f"존재하는 {file_name} 파일이 없습니다. 새로 업로드합니다.{e}")

        # S3에 업로드
        try:
            logging.info(f"Uploading {file_name} to S3")
            s3_hook.load_string(
                string_data=get_csv_string(df),
                key=key,
                bucket_name=Variable.get("s3_bucket_name"),
                replace=True,
            )  # 동일한 키가 있을 경우 덮어쓰기 설정
            logging.info(f"{file_name} S3에 업로드 완료!")
        except Exception as e:
            logging.info(f"신규 {file_name} 파일 업로드 에러: {e}")


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


get_daily_box_office >> scraping_watcha
