# 수정본
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from datetime import datetime, timedelta
import boto3
from io import StringIO
from datetime import datetime
from airflow.hooks.base import BaseHook
from airflow.hooks.S3_hook import S3Hook

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys  import  Keys
from botocore.exceptions import NoCredentialsError
import time
import pandas as pd

import os
import io
import logging

# 이미지 경로를 기반으로 서버에 요청하여 이미지 파일을 저장하게 하는 라이브러리
from urllib.request import urlopen


# s3에서 파일 리스트 불러오기 =>리뷰 파일/영화 정보 파일이 있는지 확인 위함
def read_s3_filelist():
    s3 = S3Hook(aws_conn_id='aws_conn')
    bucket_name = Variable.get("s3_bucket_name")
    
    if s3:
        logging.info("none")
    else:
        logging("conn: {conn}")
    try:
        response = s3.list_objects(Bucket=bucket_name, Prefix="naver/")
        file_list = [obj['Key'] for obj in response.get('Contents', [])]
        return file_list
    except Exception as e:
        logging.error(f"Error getting file list from S3: {e}")
        return []
    
# s3에 파일 업로드
def upload_to_s3(dataframe, file_name):
    s3_clinet = S3Hook(aws_conn_id='aws_conn')
    try:
        csv_buffer = StringIO()
        dataframe.to_csv(csv_buffer, index=False)

        s3_clinet.put_object(Body=csv_buffer.getvalue(), Bucket=Variable.get("s3_bucket_name"), Key=file_name)
        logging.info(f"File {file_name} uploaded to S3 successfully.")
    except Exception as e:
        logging.info(f"Error uploading file to S3: {e}")

# s3에 이미 파일이 존재할 경우 추가하기
def update_s3_file(dataframe, s3_key):
    # s3 파일 읽기
    s3 = S3Hook(aws_conn_id='aws_conn')
    try:
        obj = s3.get_object(Bucket=Variable.get("s3_bucket_name"), Key=s3_key)
        s3_dataframe = pd.read_csv(obj['Body'])
    except Exception as e:
        logging.info(f"Failed to read CSV file from S3: {e}")
        return

    try:
        merged_df = pd.concat([s3_dataframe, dataframe]).drop_duplicates(keep=False)
        csv_buffer = StringIO()
        merged_df.to_csv(csv_buffer, index=False)
        s3.put_object(Bucket=Variable.get("s3_bucket_name"), Key=s3_key, Body=csv_buffer.getvalue())
        logging.info("Data appended successfully.")
    except Exception as e:
        logging.info(f"Failed to Data appended S3: {e}")

# 이미지 s3 저장하기
def upload_image_to_s3(**kwargs):
    s3 = S3Hook(aws_conn_id='aws_conn')
    dic = kwargs['ti'].xcom_pull(task_ids='naver_info_crawling')
    posters_src = dic['posters_src']
    movies = dic['movies']

    for poster_src, movie in zip(posters_src, movies):
        image_data = urlopen(poster_src).read()

        bucket_name = Variable.get("s3_bucket_name")
        file_name = f"naver/naver-images/{movie}.jpg"

        try:
            s3.put_object(Body=image_data, Bucket=bucket_name, Key=file_name)
            logging.info(f'{movie} 이미지 s3에 올림')
        except Exception as e:
            logging.info(f"Error uploading image to S3: {e}")    

#s3에 있는 영진위 파일에서 영화 제목 추출하기 
def s3_to_rank_movie_list():
    try:
        s3_client = S3Hook(aws_conn_id='aws_conn')
        obj = s3_client.get_object(Bucket=Variable.get("s3_bucket_name"), Key=daily_filename())
        df = pd.read_csv(io.BytesIO(obj['Body'].read()))
        if 'movieNm' not in df.columns:
            raise ValueError("movieNm 컬럼이 데이터프레임에 존재하지 않습니다.")
        # 영진위 1-10위 영화
        movies = df['movieNm'].tolist()[-10:]
        logging.info(movies)
        return movies
    except Exception as e:
        logging.info(f"오류 발생: {e}")
        return []
    
def s3_to_naver_movie_list():
    try:
        s3_client = S3Hook(aws_conn_id='aws_conn')
        obj = s3_client.get_object(Bucket=Variable.get("s3_bucket_name"), Key='naver/naver_info.csv')
        df = pd.read_csv(io.BytesIO(obj['Body'].read()))
        if 'movie' not in df.columns:
            raise ValueError("movie 컬럼이 데이터프레임에 존재하지 않습니다.")
        movies = df['movie'].tolist()
        logging.info(movies)
        return movies
    except Exception as e:
        logging.info(f"오류 발생: {e}")
        return []

# 오늘 날짜 ex)20240216
def today_date():
    now = datetime.now()
    return now.strftime("%Y%m%d")    

# 예인님 파일 이름
def daily_filename():
    #today_ymd = today_date()
    today_ymd = "20240102"
    return f"kofic/daily-box-office/{today_ymd}.csv" 

# 네이버 크롤링하기.
def naver_info_crawling(**kwargs):
    options = Options()
    driver = webdriver.Chrome(options=options)
    
    movie_stories = []  # 영화 줄거리 정보
    naver_grades = []
    naver_male_grades = []
    naver_female_grade = []   
    naver_reviews = []
    poster_dic = {}
    naver_review_id = []
    naver_review_date = []
    naver_review_time = []


    # s3 파일명 리스트
    file_list = kwargs['ti'].xcom_pull(task_ids='read_s3_filelist')
    # 영진위 영화 리스트 1~10위                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
    movie_ranking_ten = kwargs['ti'].xcom_pull(task_ids='s3_to_rank_movie_list')
    # s3에 있는 영화 정보 파일에 이미 존재하는 영화들 => 영화 정보 안 뽑아도 되고 리뷰의 경우 모든 리뷰가 아니라 오늘의 리뷰만 뽑아서 기존파일에 덧붙이기
    naver_info_movies = kwargs['ti'].xcom_pull(task_ids='s3_to_naver_movie_list')
    new_movies  = list(set(movie_ranking_ten) - set(naver_info_movies))

    driver.get("https://www.naver.com")
    driver.implicitly_wait(2)

    for movie in movie_ranking_ten:
        # 줄거리, 이미지 정보 크롤링
        if movie in naver_info_movies:
            continue
    
        movie_info_search = f"영화 {movie} 정보"

            # 1. 영화 검색
        movie_input = driver.find_element(By.ID, "query")
        ActionChains(driver).send_keys_to_element(movie_input, movie_info_search).perform()
        time.sleep(1)
        movie_input.send_keys(Keys.RETURN)
        driver.implicitly_wait(2)

        # 영화 줄거리
        # 1. '더 보기'를 클릭해야 전체 줄거리가 나오는 경우가 있음
        try:
            more_button_xpath = "/html/body/div[3]/div[2]/div/div[1]/div[2]/div[2]/div[2]/div/div[1]/div/a"
            driver.find_element(By.XPATH, more_button_xpath).click()
            driver.implicitly_wait(2)
        except Exception as e:
            pass
        
        movie_info_tab = driver.find_element(By.CLASS_NAME, "cm_content_wrap")
        # intro_box _content  -> text _content_text
        movie_story_selector = "div.cm_content_area._cm_content_area_synopsis > div > div.intro_box._content > p"
        movie_story = movie_info_tab.find_element(By.CSS_SELECTOR, movie_story_selector)
        movie_stories.append(movie_story.text)
        logging.info(f'{movie} 줄거리 추출함')

                    # 영화 포스터 이미지 src 찾기
        movie_poster_selector = "div.cm_content_area._cm_content_area_info > div > div.detail_info > a > img"
        movie_poster = driver.find_element(By.CSS_SELECTOR, movie_poster_selector)
        poster_src = movie_poster.get_attribute('src')
        poster_dic[movie] = poster_src
        logging.info(poster_src)

        driver.get("https://www.naver.com")

    movie_nm = []   # 각각 해당하는 영화명(리뷰) 

            # 영화 리뷰, 평점 크롤링
    for movie in movie_ranking_ten:    
        movie_review_search = f"영화 {movie} 관람평"
                # 1. 영화 검색
        movie_input = driver.find_element(By.ID, "query")
        ActionChains(driver).send_keys_to_element(movie_input, movie_review_search).perform()
        time.sleep(1)
        movie_input.send_keys(Keys.RETURN)
        driver.implicitly_wait(2)

        if movie not in naver_info_movies:
            grade_reviews_tab = driver.find_element(By.CLASS_NAME, "cm_pure_box.lego_rating_slide_outer")
                    # 평점 추출
            grade = grade_reviews_tab.find_element(By.CLASS_NAME, "lego_rating_box_see")
                # 관람객 평점
            entire_grade = grade.find_element(By.CSS_SELECTOR, "div.area_intro_info > span.area_star_number").text[:-2]
                    # 남성 관람객 평점
            male_grade = grade.find_element(By.CSS_SELECTOR, "div.area_gender > div.area_card_male > span.area_star_number > span").text
                    # 여성 관람객 평점
            female_grade = grade.find_element(By.CSS_SELECTOR, "div.area_gender > div.area_card_female > span.area_star_number > span").text
            
            naver_grades.append(entire_grade)
            naver_male_grades.append(male_grade)
            naver_female_grade.append(female_grade)
            logging.info(entire_grade, male_grade, female_grade)

                    # 리뷰 추출
        review_list = driver.find_element(By.CLASS_NAME, "lego_review_list._scroller")
        before_h = driver.execute_script('return arguments[0].scrollHeight;', review_list)

        while True:
                        # 맨 아래로 스크롤을 내린다.
            driver.execute_script('arguments[0].scrollBy(0, arguments[0].scrollHeight);', review_list)
                        # 스크롤하는 동안의 페이지 로딩 시간
            time.sleep(1)

                        # 스크롤 후 높이
            after_h = driver.execute_script('return arguments[0].scrollHeight;', review_list)
            if after_h == before_h:
                break
            before_h = after_h

        review_list = review_list.find_element(By.CLASS_NAME, "area_card_outer._item_wrapper")
        reviews = review_list.find_elements(By.CLASS_NAME, "area_card._item")

        #영화별 리뷰 추출
        movie_review = []
        movie_review_date = []
        movie_review_time = []
        movie_review_id = []

        if movie not in naver_info_movies:
            for i in range(len(reviews)):
                review_content = reviews[i].get_attribute("data-report-title")
                review_date = reviews[i].get_attribute('data-report-time')[:8]  # 작성 날짜 
                review_time = reviews[i].get_attribute("data-report-time")[-5:] # 작성 시간
                review_id = reviews[i].get_attribute("data-report-writer-id")   # 작성 아이디
                # 전체 리뷰 추출
                if review_content != " ":
                    movie_nm.append(movie)
                    naver_reviews.append(review_content)
                    naver_review_id.append(review_id)
                    naver_review_date.append(review_date)
                    naver_review_time.append(review_time)

                    movie_review.append(review_content)
                    movie_review_date.append(review_date)
                    movie_review_id.append(review_id)
                    movie_review_time.append(review_time)
        else:
            for i in range(len(reviews)):
                review_content = reviews[i].get_attribute("data-report-title")
                review_date = reviews[i].get_attribute('data-report-time')[:8]  # 작성 날짜 
                review_time = reviews[i].get_attribute("data-report-time")[-5:] # 작성 시간
                review_id = reviews[i].get_attribute("data-report-writer-id")   # 작성 아이디

                score = reviews[i].find_element(By.CLASS_NAME, "area_text_box").text[-2:]
                # 별점 10점이면서 오늘 날짜의 리뷰만 추출
                if review_date == today_date():
                    if review_content != " " and score == "10":
                        movie_nm.append(movie)
                        naver_reviews.append(review_content)
                        naver_review_id.append(review_id)
                        naver_review_date.append(review_date)
                        naver_review_time.append(review_time)

                        movie_review.append(review_content)
                        movie_review_date.append(review_date)
                        movie_review_id.append(review_id)
                        movie_review_time.append(review_time)
        
        df = pd.DataFrame({
            'movie' : [movie]*len(movie_review),
            'naver_review' : movie_review,
            'id' : movie_review_id,
            'review_date' : movie_review_date,
            'review_date_time' : movie_review_time,
        })
        upload_to_s3(df, f'naver/{movie}_review.csv')

       
        '''for i in range(len(reviews)):
            review_content = reviews[i].get_attribute("data-report-title")
            review_date = reviews[i].get_attribute('data-report-time')[:8]  # 작성 날짜 
            review_time = reviews[i].get_attribute("data-report-time")[-5:] # 작성 시간
            review_id = reviews[i].get_attribute("data-report-writer-id")   # 작성 아이디

            score = reviews[i].find_element(By.CLASS_NAME, "area_text_box").text[-2:]
                # 별점 10점이면서 전체 리뷰 추출
            if review_content != " " and score == "10":
                movie_nm.append(movie)
                naver_reviews.append(review_content)
                naver_review_id.append(review_id)
                naver_review_date.append(review_date)
                naver_review_time.append(review_time)'''


        driver.get("https://www.naver.com")            

        # 영화 줄거리, 평점 dataframe
    naver_movie_info = pd.DataFrame({
        'movie' : new_movies,
        'story' : movie_stories,
        'entire_grade' : naver_grades,
        'male_grade' : naver_male_grades,
        'female_grade' : naver_female_grade,
    })

        # 영화 리뷰 dataframe
    naver_reviews = pd.DataFrame({
        'movie' : movie_nm,
        'naver_review' : naver_reviews,
        'id' : naver_review_id,
        'review_date' : naver_review_date,
        'review_date_time' : naver_review_time,
    })

    '''if 'naver/naver_info.csv' not in file_list:
        upload_to_s3(naver_movie_info, 'naver/naver_info.csv')
    else:
        update_s3_file(naver_movie_info,'naver/naver_info.csv')

        
    if 'naver/naver_reviews.csv' not in file_list:
        upload_to_s3(naver_reviews, 'naver/naver_reviews.csv')
    else:
        update_s3_file(naver_reviews,'naver/naver_reviews.csv')'''

    upload_to_s3(naver_movie_info, 'naver/naver_info.csv')
    upload_to_s3(naver_reviews, 'naver/naver_reviews.csv')

    return poster_dic

default_args = {
    'start_date': datetime(2024,2,18),
    'retries': 1,
    'retry_delay': timedelta(minutes=20), 
    
}

with DAG(
    dag_id = "naver_crawler",
    schedule_interval='50 23 * * 1-5',  # 평일 실행 
    catchup = False,
    default_args=default_args,
) as dag:
    
    read_s3_filelist = PythonOperator(
        task_id = 'read_s3_filellist',
        python_callable=read_s3_filelist,
        dag=dag
    )
    s3_to_rank_movie_list = PythonOperator(
        task_id = 's3_to_rank_movie_list',
        python_callable=s3_to_rank_movie_list,
        dag=dag
    )
    s3_to_naver_movie_list = PythonOperator(
        task_id = 's3_to_naver_movie_list',
        python_callable=s3_to_naver_movie_list,
        dag=dag
    )
    naver_info_crawling = PythonOperator(
        task_id = 'naver_info_crawling',
        python_callable=naver_info_crawling,
        dag=dag,
    )
    upload_image_to_s3 = PythonOperator(
        task_id = 'upload_image_to_s3',
        python_callable=upload_image_to_s3,
        dag=dag,
    )
    read_s3_filelist >> s3_to_rank_movie_list, s3_to_naver_movie_list >> naver_info_crawling >> upload_image_to_s3
