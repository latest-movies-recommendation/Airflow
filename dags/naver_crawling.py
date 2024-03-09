import logging
import time
from datetime import date, datetime, timedelta
from io import StringIO

import pandas as pd
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver import ActionChains
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys


# s3에서 파일 리스트 불러오기 =>리뷰 파일/영화 정보 파일이 있는지 확인 위함 ㄱ
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
        s3_dataframe["movieCd"] = s3_dataframe["movieCd"].astype(str)
        dataframe["movieCd"] = dataframe["movieCd"].astype(str)

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


def update_s3_score_file(dataframe, s3_key, movies):
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
        s3_dataframe["movieCd"] = s3_dataframe["movieCd"].astype(str)
        dataframe["movieCd"] = dataframe["movieCd"].astype(str)

        # 원래 파일에 있던 영화 삭제 후 다시 평점 업데이트
        for movie in movies:
            s3_dataframe = s3_dataframe[s3_dataframe["movieNm"] != movie]

        merged_df = pd.concat([s3_dataframe, dataframe])

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


# s3에 있는 영진위 파일에서 영화 제목 추출하기
def s3_to_rank_movie_list():
    movies = {}
    try:
        s3 = S3Hook(aws_conn_id="aws_conn")
        obj = s3.get_key(
            key=daily_filename(), bucket_name=Variable.get("s3_bucket_name")
        )
        if obj:
            csv_data = obj.get()["Body"].read().decode("utf-8")
            df = pd.read_csv(StringIO(csv_data))

            if "movieNm" not in df.columns:
                raise ValueError("movieNm 컬럼이 데이터프레임에 존재하지 않습니다.")
            # 영진위 1-10위 영화
            df_last_10 = df.tail(10)
            for _, row in df_last_10.iterrows():
                movies[row["movieCd"]] = row["movieNm"]
            return movies
        else:
            return None
    except Exception as e:
        logging.info(f"오류 발생: {e}")
        return []


def s3_to_naver_movie_list():
    try:
        s3 = S3Hook(aws_conn_id="aws_conn")
        obj = s3.get_key(
            key="naver/naver_info.csv", bucket_name=Variable.get("s3_bucket_name")
        )
        if obj:
            csv_data = obj.get()["Body"].read().decode("utf-8")
            df = pd.read_csv(StringIO(csv_data))

            if "movieNm" not in df.columns:
                raise ValueError("movieNm 컬럼이 데이터프레임에 존재하지 않습니다.")
            movies = df["movieNm"].tolist()
            logging.info(movies)
            return movies
        else:
            return None
    except Exception as e:
        logging.info(f"오류 발생: {e}")
        return []


# 오늘 날짜 ex)20240216
def today_date():
    day = date.today() - timedelta(1)
    return day.strftime("%Y%m%d")


# 예인님 파일 이름
def daily_filename():
    today_ymd = today_date()
    return f"kofic/daily-box-office/{today_ymd}.csv"


# 네이버 크롤링하기.
def naver_info_crawling(**kwargs):
    options = Options()
    options.add_argument("--headless")
    options.add_argument("window-size=1200x600")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=options)

    movieNm = []
    movieCd = []
    movie_posters = []
    movie_stories = []
    movie_groups = []
    movie_companies = []
    movie_genres = []
    movie_times = []
    movie_countries = []

    # s3 파일명 리스트
    file_list = read_s3_filelist()
    # 영진위 영화 리스트 1~10위
    movie_ranking_ten = kwargs["ti"].xcom_pull(task_ids="s3_to_rank_movie_list")
    # s3에 있는 영화 정보 파일에 이미 존재하는 영화들 => 영화 정보 안 뽑아도 되고 리뷰의 경우 모든 리뷰가 아니라 오늘의 리뷰만 뽑아서 기존파일에 덧붙이기
    naver_info_movies = kwargs["ti"].xcom_pull(task_ids="s3_to_naver_movie_list")

    movies = []
    codes = []

    for k, v in movie_ranking_ten.items():
        if v in naver_info_movies:
            continue
        codes.append(k)
        movies.append(v)

    driver.get("https://www.naver.com")
    driver.implicitly_wait(2)

    for movie, code in zip(movies, codes):
        # 줄거리, 이미지 정보 크롤링
        movie_info_search = f"영화 {movie} 정보"

        # 1. 영화 검색
        movie_input = driver.find_element(By.ID, "query")
        ActionChains(driver).send_keys_to_element(
            movie_input, movie_info_search
        ).perform()
        time.sleep(1)
        movie_input.send_keys(Keys.RETURN)
        driver.implicitly_wait(2)

        # 영화 줄거리
        # 1. '더 보기'를 클릭해야 전체 줄거리가 나오는 경우가 있음
        try:
            more_button_xpath = "/html/body/div[3]/div[2]/div/div[1]/div[2]/div[2]/div[2]/div/div[1]/div/a"
            driver.find_element(By.XPATH, more_button_xpath).click()
            driver.implicitly_wait(2)
        except NoSuchElementException:
            pass

        try:
            movie_info_tab = driver.find_element(By.CLASS_NAME, "cm_content_wrap")
            # 영화 줄거리 추출
            movie_story_selector = "div.cm_content_area._cm_content_area_synopsis > div > div.intro_box._content > p"
            movie_story = movie_info_tab.find_element(
                By.CSS_SELECTOR, movie_story_selector
            )
            logging.info(f"{movie} 줄거리 추출함")

            # 영화 포스터 이미지 src 찾기
            movie_info_selector = "div.cm_content_area._cm_content_area_info"

            movie_info = driver.find_element(By.CSS_SELECTOR, movie_info_selector)

            movie_poster = movie_info.find_element(
                By.CSS_SELECTOR, "div > div.detail_info > a > img"
            )
            poster_src = movie_poster.get_attribute("src")

            movieNm.append(movie)
            movieCd.append(code)
            movie_stories.append(movie_story.text)
            movie_posters.append(poster_src)

            try:
                movie_group = movie_info.find_element(
                    By.CSS_SELECTOR,
                    "div > div.detail_info > dl > div:nth-child(2) > dd",
                )
                movie_groups.append(movie_group.text)
            except NoSuchElementException:
                movie_groups.append("모름")

            try:
                movie_genre = movie_info.find_element(
                    By.CSS_SELECTOR,
                    "div > div.detail_info > dl > div:nth-child(3) > dd",
                )
                movie_genres.append(movie_genre.text)
            except NoSuchElementException:
                movie_genres.append("모름")

            try:
                movie_country = movie_info.find_element(
                    By.CSS_SELECTOR,
                    "div > div.detail_info > dl > div:nth-child(4) > dd",
                )
                movie_countries.append(movie_country.text)
            except NoSuchElementException:
                movie_countries.append("모름")

            try:
                movie_time = movie_info.find_element(
                    By.CSS_SELECTOR,
                    "div > div.detail_info > dl > div:nth-child(5) > dd",
                )
                movie_times.append(movie_time.text)
            except NoSuchElementException:
                movie_times.append("모름")

            try:
                movie_company = movie_info.find_element(
                    By.CSS_SELECTOR,
                    "div > div.detail_info > dl > div:nth-child(6) > dd",
                )
                movie_companies.append(movie_company.text)
            except NoSuchElementException:
                movie_companies.append("모름")
        except NoSuchElementException:
            pass

        driver.get("https://www.naver.com")

    naver_info_df = pd.DataFrame(
        {
            "movieNm": movieNm,
            "movieCd": movieCd,
            "story": movie_stories,
            "accessible": movie_groups,
            "genre": movie_genres,
            "country": movie_countries,
            "running_time": movie_times,
            "company": movie_companies,
            "poster": movie_posters,
        }
    )

    if "naver/naver_info.csv" in file_list:
        update_s3_file(naver_info_df, "naver/naver_info.csv")
    else:
        upload_to_s3(naver_info_df, "naver/naver_info.csv")


def critic_review_crawling(**kwargs):
    options = Options()
    options.add_argument("--headless")
    options.add_argument("window-size=1200x600")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=options)

    movie_ranking_ten = kwargs["ti"].xcom_pull(task_ids="s3_to_rank_movie_list")

    movieNm = []
    movieCd = []

    for k, v in movie_ranking_ten.items():
        movieCd.append(k)
        movieNm.append(v)

    info_critic_score = {}
    driver.get("https://www.naver.com")
    driver.implicitly_wait(2)

    for movie, code in zip(movieNm, movieCd):
        name = []
        critic_review = []
        critic_score = []
        movie_review_search = f"영화 {movie} 관람평"

        # 영화 검색
        movie_input = driver.find_element(By.ID, "query")
        ActionChains(driver).send_keys_to_element(
            movie_input, movie_review_search
        ).perform()
        movie_input.send_keys(Keys.RETURN)
        time.sleep(1)

        try:
            link_pa = "/html/body/div[3]/div[2]/div/div[1]/div[2]/div[2]/div/div/div[1]/div/div/ul"
            tabs = driver.find_element(By.XPATH, link_pa).find_elements(
                By.CLASS_NAME, "tab._tab"
            )

            for tab in tabs:
                who = tab.get_attribute("data-tab")
                if who == "critics":
                    tab.find_element(By.TAG_NAME, "a").click()
                    time.sleep(1)
                    break
        except NoSuchElementException:
            info_critic_score[movie] = "평점 없음"
            driver.get("https://www.naver.com")
            continue

        review_list = driver.find_element(By.CLASS_NAME, "lego_critic_outer._scroller")
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

        try:
            critic_reviews_list = driver.find_element(By.CLASS_NAME, "area_ulist")
            critic_reviews = critic_reviews_list.find_elements(By.TAG_NAME, "li")

            num = 0

            for e in critic_reviews:
                critic_info = e.find_elements(By.CLASS_NAME, "area_dlist_info")[0]
                critic_name = critic_info.find_element(
                    By.CLASS_NAME, "this_info_name"
                ).text

                time.sleep(1)
                star = (
                    e.find_element(By.CLASS_NAME, "lego_movie_pure_star")
                    .find_element(By.CLASS_NAME, "area_text_box")
                    .text[12:]
                )
                star = star.replace("\n", "")
                num += float(star)

                review_button = "div[3]/button"
                try:
                    e.find_element(By.XPATH, review_button).click()
                    time.sleep(1)
                except NoSuchElementException:
                    pass

                review_path = "div[3]/span"
                review_con = e.find_element(By.XPATH, review_path).text
                name.append(critic_name)
                critic_review.append(review_con)
                critic_score.append(star)
                logging.info(name)
                logging.info(critic_review)
                logging.info(critic_score)

            final_score = round((num / len(critic_score)), 1)
            info_critic_score[movie] = final_score
            logging.info(f"평점:{info_critic_score}")

            critic_review_df = pd.DataFrame(
                {
                    "movieNm": [movie] * len(name),
                    "movieCd": [code] * len(name),
                    "name": name,
                    "review": critic_review,
                    "score": critic_score,
                }
            )

            upload_to_s3(
                critic_review_df,
                f"naver/naver-critic-reviews/{movie}_critic_review.csv",
            )
        except NoSuchElementException:
            info_critic_score[movie] = "평점 없음"
        driver.get("https://www.naver.com")

    return info_critic_score


def naver_review_crawling(**kwargs):
    options = Options()
    options.add_argument("--headless")
    options.add_argument("window-size=1200x600")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=options)

    # 평점
    naver_grades = []
    naver_male_grades = []
    naver_female_grades = []

    # 댓글
    movie_nm = []
    naver_reviews = []
    naver_review_id = []
    naver_review_date = []
    naver_review_time = []
    naver_review_score = []

    # s3 파일명 리스트
    file_list = read_s3_filelist()
    # 영진위 영화 리스트 1~10위
    dic = kwargs["ti"].xcom_pull(task_ids="s3_to_rank_movie_list")
    movies = dict(map(reversed, dic.items()))  # key에 영화명, value에 영화코드

    critic_score = kwargs["ti"].xcom_pull(task_ids="critic_review_crawling")
    naver_exists_movies = kwargs["ti"].xcom_pull(task_ids="s3_to_naver_movie_list")

    movieNm = []
    movieCd = []
    movie_critic_score = []

    movie_name = []
    movie_code = []
    movie_star = []

    for k, v in critic_score.items():
        movie_name.append(k)
        movie_code.append(movies.get(k))
        movie_star.append(v)

    driver.get("https://www.naver.com")
    driver.implicitly_wait(2)
    # 영화 리뷰, 평점 크롤링
    for movie, code, star in zip(movie_name, movie_code, movie_star):
        # 영화별 댓글 추출
        movie_review = []
        movie_review_date = []
        movie_review_time = []
        movie_review_id = []
        movie_score = []

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
            grade_reviews_tab = driver.find_element(
                By.CLASS_NAME, "cm_pure_box.lego_rating_slide_outer"
            )
            grade = grade_reviews_tab.find_element(By.CLASS_NAME, "lego_rating_box_see")

            entire_grade = grade.find_element(
                By.CSS_SELECTOR, "div.area_intro_info > span.area_star_number"
            ).text[:-2]
            male_grade = grade.find_element(
                By.CSS_SELECTOR,
                "div.area_gender > div.area_card_male > span.area_star_number > span",
            ).text
            female_grade = grade.find_element(
                By.CSS_SELECTOR,
                "div.area_gender > div.area_card_female > span.area_star_number > span",
            ).text

            naver_grades.append(entire_grade)
            naver_male_grades.append(male_grade)
            naver_female_grades.append(female_grade)

            movieNm.append(movie)
            movieCd.append(code)
            movie_critic_score.append(star)

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

                    # 영화별 리뷰 전체 저장
                    movie_review.append(review_content)
                    movie_review_date.append(review_date)
                    movie_review_id.append(review_id)
                    movie_review_time.append(review_time)
                    movie_score.append(review_score)

                    # 리뷰 파일
                    if review_score == "10" or review_score == "1":
                        movie_nm.append(movie)
                        naver_review_id.append(review_id)
                        naver_review_date.append(review_date)
                        naver_review_time.append(review_time)
                        naver_reviews.append(review_content)
                        naver_review_score.append(review_score)

            movie_review = pd.DataFrame(
                {
                    "movie": [movie] * len(movie_review),
                    "id": movie_review_id,
                    "naver_review": movie_review,
                    "review_date": movie_review_date,
                    "review_date_time": movie_review_time,
                    "score": movie_score,
                }
            )

            upload_to_s3(movie_review, f"naver/naver-reviews/{movie}_review.csv")
            driver.get("https://www.naver.com")

        except Exception:
            driver.get("https://www.naver.com")

    # 영화 리뷰 dataframe
    naver_movie_reviews = pd.DataFrame(
        {
            "movieNm": movie_nm,
            "id": naver_review_id,
            "naver_review": naver_reviews,
            "review_date": naver_review_date,
            "review_date_time": naver_review_time,
            "review_score": naver_review_score,
        }
    )

    naver_movie_score = pd.DataFrame(
        {
            "movieNm": movieNm,
            "movieCd": movieCd,
            "entire_grade": naver_grades,
            "male_grade": naver_male_grades,
            "female_grade": naver_female_grades,
            "critic_grade": movie_critic_score,
        }
    )

    if "naver/naver_reviews.csv" in file_list:
        update_s3_file(naver_movie_reviews, "naver/naver_reviews.csv")
    else:
        upload_to_s3(naver_movie_reviews, "naver/naver_reviews.csv")

    if "naver/naver_movie_score.csv" in file_list:
        update_s3_score_file(
            naver_movie_score, "naver/naver_movie_score.csv", naver_exists_movies
        )
    else:
        upload_to_s3(naver_movie_score, "naver/naver_movie_score.csv")


def s3_to_postgres():
    s3 = S3Hook(aws_conn_id="aws_conn")
    bucket_name = Variable.get("s3_bucket_name")

    s3_key = "naver/naver_info.csv"

    # S3에서 CSV 파일 읽기
    obj = s3.get_key(key=s3_key, bucket_name=bucket_name)
    if obj:
        csv_data = obj.get()["Body"].read().decode("utf-8")
        s3_dataframe = pd.read_csv(StringIO(csv_data))
        data = s3_dataframe.where(pd.notnull(s3_dataframe), None)

    # PostgreSQL Hook을 사용하여 연결
    postgres_hook = PostgresHook(postgres_conn_id="postgres_conn")
    conn = postgres_hook.get_conn()
    cur = conn.cursor()

    try:
        # 테이블이 이미 존재하는지 확인
        check_table_query = "SELECT to_regclass('movie_info')"
        cur.execute(check_table_query)
        result = cur.fetchone()[0]

        if result is None:
            # 테이블이 존재하지 않으면 새로운 테이블 생성
            create_table_query = f'CREATE TABLE movie_info ({", ".join(f"{col} VARCHAR" for col in data.columns)})'
            cur.execute(create_table_query)
            logging.info("Table movie_info created.")

        # 테이블에 데이터가 있으면 지우고 삽입하도록 설정
        if not data.empty:
            delete_query = "DELETE FROM movie_info"
            cur.execute(delete_query)

        insert_query = f"INSERT INTO movie_info ({', '.join(data.columns)}) VALUES ({', '.join(['%s'] * len(data.columns))})"
        for _, row in data.iterrows():
            cur.execute(insert_query, tuple(row))
            logging.info("SQL insert start")

        conn.commit()
        logging.info("Data successfully inserted into PostgreSQL RDS.")
    except Exception as e:
        logging.error(f"Error: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()


default_args = {
    "start_date": datetime(2024, 2, 18),
    "retries": 1,
    "retry_delay": timedelta(minutes=20),
}

with DAG(
    dag_id="naver_crawler",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
) as dag:

    trigger_naver = TriggerDagRunOperator(
        task_id="trigger_naver",
        trigger_dag_id="daily_box_office_elt",
    )
    s3_to_rank_movie_list = PythonOperator(
        task_id="s3_to_rank_movie_list", python_callable=s3_to_rank_movie_list, dag=dag
    )
    s3_to_naver_movie_list = PythonOperator(
        task_id="s3_to_naver_movie_list",
        python_callable=s3_to_naver_movie_list,
        dag=dag,
    )
    naver_info_crawling = PythonOperator(
        task_id="naver_info_crawling",
        python_callable=naver_info_crawling,
        dag=dag,
    )
    critic_review_crawling = PythonOperator(
        task_id="critic_review_crawling",
        python_callable=critic_review_crawling,
        dag=dag,
    )
    naver_review_crawling = PythonOperator(
        task_id="naver_review_crawling",
        python_callable=naver_review_crawling,
        dag=dag,
    )
    s3_to_postgres = PythonOperator(
        task_id="s3_to_postgres",
        python_callable=s3_to_postgres,
        dag=dag,
    )
    trigger_naver >> s3_to_rank_movie_list, s3_to_naver_movie_list
    s3_to_rank_movie_list, s3_to_naver_movie_list >> naver_info_crawling
    naver_info_crawling >> s3_to_postgres
    s3_to_postgres >> critic_review_crawling
    critic_review_crawling >> naver_review_crawling
