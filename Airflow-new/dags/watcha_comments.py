# <<<<<<< yein/daily-box-office
# import logging
# import time
# from datetime import datetime, timedelta
# from io import StringIO

# import boto3
# import pandas as pd
# =======
# #수정본
# >>>>>>> develop
# from airflow import DAG
# from airflow.hooks.S3_hook import S3Hook
# from airflow.models import Variable
# from airflow.operators.python import PythonOperator
# from selenium import webdriver
# from selenium.common.exceptions import NoSuchElementException
# from selenium.webdriver.common.by import By


# def get_titles(ti, **kwargs):
#     s3_hook = S3Hook(aws_conn_id="aws_conn")
#     s3_key = "kofic/daily-box-office/20240216.csv"
#     try:
#         obj = s3_hook.get_key(key=s3_key, bucket_name=Variable.get("s3_bucket_name"))
#         if obj:
#             # CSV 파일 데이터를 Pandas DataFrame으로 읽어오기
#             csv_data = obj.get()["Body"].read().decode("utf-8")
#             df = pd.read_csv(StringIO(csv_data))
#             logging.info("CSV file downloaded and converted to DataFrame successfully.")
#             if "movieNm" not in df.columns:
#                 raise ValueError("movieNm Column does not exist in the dataframe.")
#             # 영진위 1-10위 영화
#             movies = df["movieNm"].tolist()[-10:]
#             ti.xcom_push(key="movies_title", value=movies)
#             logging.info(movies)
#             return movies
#         else:
#             logging.info("File not found in S3.")
#             return None
#     except Exception as e:
#         logging.info(f"Data load failed from s3: {e}")


# def access_watcha(title, **kwargs):
#     try:
#         chrome_options = webdriver.ChromeOptions()
#         chrome_options.add_experimental_option("detach", True)

#         driver = webdriver.Chrome(
#             "/opt/homebrew/bin/chromedriver", options=chrome_options
#         )
#         driver.set_page_load_timeout(30)

#         url = f"https://pedia.watcha.com/ko-KR/search?query={title}"
#         driver.get(url)
#         time.sleep(3)
#         logging.info(f"{title}으로 Watcha에 접근중")
#         return driver
#     except Exception as e:
#         logging.info(f"Fail to Access Watcha: {e}")
#     # df = comments(title, driver)
#     # print(df)


# def actual_access_watcha_logic(title, **kwargs):
#     try:
#         chrome_options = webdriver.ChromeOptions()
#         chrome_options.add_experimental_option("detach", True)
#         driver = webdriver.Chrome(
#             "/opt/homebrew/bin/chromedriver", options=chrome_options
#         )
#         driver.set_page_load_timeout(30)
#         url = f"https://pedia.watcha.com/ko-KR/search?query={title}"
#         driver.get(url)
#         time.sleep(3)
#         logging.info(f"{title}으로 Watcha에 접근중")
#         # 여기서 필요한 작업 수행...
#         return driver
#     except Exception as e:
#         logging.info(f"Fail to Access Watcha: {e}")


# def access_watcha_wrapper(**kwargs):
#     ti = kwargs["ti"]
#     titles = ti.xcom_pull(task_ids="get_titles", key="movies_title")
#     if titles is not None:
#         for title in titles:
#             actual_access_watcha_logic(title, **kwargs)
#     else:
#         logging.info("No titles were retrieved.")


# # def page_scrolling(driver):
# #     scroll_location = driver.execute_script("return document.body.scrollHeight")

# #     while True:
# #         #현재 스크롤의 가장 아래로 내림
# #         driver.execute_script("window.scrollTo(0,document.body.scrollHeight)")

# #         #전체 스크롤이 늘어날 때까지 대기
# #         time.sleep(2)

# #         #늘어난 스크롤 높이
# #         scroll_height = driver.execute_script("return document.body.scrollHeight")

# #         #늘어난 스크롤 위치와 이동 전 위치 같으면(더 이상 스크롤이 늘어나지 않으면) 종료
# #         if scroll_location == scroll_height:
# #             break

# #         #같지 않으면 스크롤 위치 값을 수정하여 같아질 때까지 반복
# #         else:
# #             #스크롤 위치값을 수정
# #             scroll_location = driver.execute_script("return document.body.scrollHeight")


# # def get_comments(title, driver):
# #     a = driver.find_element(By.CLASS_NAME, "e1ic68ft4")
# #     link = a.get_attribute("href") + "/comments?order=recent"
# #     driver.get(link)
# #     time.sleep(5)
# #     page_scrolling(driver)
# #     time.sleep(5)

# #     reviews = driver.find_elements(By.CLASS_NAME, "egj9y8a4")
# #     data = []

# #     # 상위 200개의 리뷰만 가져오기
# #     for index, review in enumerate(reviews):
# #         if index >= 200:
# #             break

# #         id = review.find_element(By.CLASS_NAME, "eovgsd00").text
# #         like = review.find_element(By.TAG_NAME, "em").text
# #         try:
# #             score_element = review.find_element(By.CLASS_NAME, "egj9y8a0")
# #             score = score_element.text if score_element.text != "보고싶어요" else None
# #         except NoSuchElementException:
# #             score = None

# #         comment = review.find_element(By.CLASS_NAME, "e1hvy88212").text.replace("\n", " ").replace('\"',"")

# #         # 각 리뷰의 정보를 리스트에 추가합니다.
# #         data.append([id, score, like, comment])

# #     # 데이터 리스트를 DataFrame으로 변환합니다.
# #     df = pd.DataFrame(data, columns=['id', 'score', 'like', 'comment'])
# #     return df


# # def upload_to_s3(df, file_name):
# #     s3_hook = S3Hook(aws_conn_id='aws_conn')
# #     try:
# #         s3_hook.load_file(
# #             file_name=file_name,
# #             key=key,
# #             bucket_name=s3_bucket_name,
# #             replace=True
# #         )

# #         csv_buffer = StringIO()
# #         df.to_csv(csv_buffer, index=False)
# #         s3_client.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=file_name)
# #         logging.info(f"File {file_name} uploaded to S3 successfully.")
# #     except Exception as e:
# #         logging.info(f"Error uploading file to S3: {e}")


# dag = DAG(
#     dag_id="watcha_comments",
#     start_date=datetime(2024, 2, 14),
#     schedule="0 9 * * *",
#     catchup=False,
#     max_active_runs=1,
#     default_args={
#         "retries": 1,
#         "retry_delay": timedelta(minutes=3),
#     },
# )

# get_titles = PythonOperator(
#     task_id="get_titles", python_callable=get_titles, provide_context=True, dag=dag
# )

# access_watcha = PythonOperator(
#     task_id="access_watcha",
#     python_callable=access_watcha_wrapper,
#     provide_context=True,
#     dag=dag,
# )

# # get_comments = PythonOperator(
# #     task_id='get_comments',
# #     python_callable=get_comments,
# #     dag=dag
# # )

# # upload_to_s3 = PythonOperator(
# #     task_id = 'upload_to_s3',
# #     python_callable = upload_to_s3,
# #     provide_context = True,
# #     op_kwargs = {
# #             'filename' : '/opt/airflow/dags/jumpit.csv',
# #             'key': 'path/in/s3/bucket/jobinfo_jumpit.csv',
# #             'bucket_name' : 'de-4-2-dev-bucket'
# #     },
# #     dag = dag
# # )


# get_titles >> access_watcha
# # access_watcha >> get_comments >> upload_to_s3
