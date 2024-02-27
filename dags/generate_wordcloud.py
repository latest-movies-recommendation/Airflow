import logging
from datetime import datetime, timedelta
from io import StringIO

import matplotlib.pyplot as plt
import pandas as pd
from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from konlpy.tag import Okt
from wordcloud import WordCloud


def yesterday_date():
    now = datetime.now() - timedelta(days=1)
    return now.strftime("%Y%m%d")


# DAG 정의
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "wordcloud_generation",
    default_args=default_args,
    description="Generate wordcloud from daily box office file",
    schedule_interval=timedelta(days=1),
    catchup=False,
)


def get_daily_box_office(ti, **kwargs):
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


def download_file_from_s3(**kwargs):
    ti = kwargs["ti"]
    titles = ti.xcom_pull(task_ids="get_daily_box_office", key="movies_title")

    s3_hook = S3Hook(aws_conn_id="aws_default")
    bucket_name = "your-bucket-name"
    for title in titles:
        key = f"data/{title}.csv"
        local_path = f"/tmp/{title}.csv"
        s3_hook.download_file(key=key, bucket_name=bucket_name, local_path=local_path)
        ti.xcom_push(key=f"local_path_{title}", value=local_path)


def generate_wordcloud(**kwargs):
    ti = kwargs["ti"]
    titles = ti.xcom_pull(task_ids="get_daily_box_office", key="movies_title")
    logging.info(titles)

    for title in titles:
        local_path = ti.xcom_pull(key=f"local_path_{title}")
        df = pd.read_csv(local_path, encoding="utf-8")

        df["comment"] = df["comment"].str.replace("[^가-힣]", " ", regex=True)
        df["comment"] = df["comment"].astype(str)
        df.dropna(subset=["comment"], inplace=True)

        okt = Okt()
        nouns = df["comment"].apply(okt.nouns).explode().dropna()
        stopwords = set(["영화", "내가", "무엇"])
        nouns = [word for word in nouns if word not in stopwords]

        df_word = pd.DataFrame({"word": nouns})
        df_word["count"] = df_word["word"].str.len()
        df_word = df_word[df_word["count"] >= 2]
        df_word = (
            df_word.groupby("word", as_index=False)
            .count()
            .sort_values("count", ascending=False)
        )
        df_word = df_word[df_word["count"] >= 5]

        dic_word = df_word.set_index("word").to_dict()["count"]
        wc = WordCloud(
            font_path="/usr/share/fonts/truetype/nanum/NanumGothic.ttf",
            random_state=123,
            width=400,
            height=400,
            background_color="white",
        ).generate_from_frequencies(dic_word)

        plt.figure(figsize=(10, 10))
        plt.axis("off")
        plt.imshow(wc)
        plt.savefig(f"/tmp/wordcloud_{title}.png")
        df_word.to_csv(f"/tmp/processed_{title}.csv", index=False)


def upload_to_s3(**kwargs):
    ti = kwargs["ti"]
    titles = ti.xcom_pull(task_ids="get_daily_box_office", key="movies_title")
    s3_hook = S3Hook(aws_conn_id="aws_default")
    bucket_name = Variable.get("s3_bucket_name")

    for title in titles:
        wordcloud_image_path = f"/tmp/wordcloud_{title}.png"
        processed_csv_path = f"/tmp/processed_{title}.csv"

        # 이미지 업로드
        with open(wordcloud_image_path, "rb") as f:
            s3_hook.load_file(
                filename=wordcloud_image_path,
                key=f"image/wordcloud_{title}.png",
                bucket_name=bucket_name,
                replace=True,
            )

        # CSV 업로드
        with open(processed_csv_path, "rb") as f:
            s3_hook.load_file(
                filename=processed_csv_path,
                key=f"dict/processed_{title}.csv",
                bucket_name=bucket_name,
                replace=True,
            )


# Task 정의
download_task = PythonOperator(
    task_id="download_file_from_s3",
    python_callable=download_file_from_s3,
    provide_context=True,
    dag=dag,
)

generate_wordcloud_task = PythonOperator(
    task_id="generate_wordcloud",
    python_callable=generate_wordcloud,
    provide_context=True,
    dag=dag,
)

upload_to_s3_task = PythonOperator(
    task_id="upload_to_s3",
    python_callable=upload_to_s3,
    provide_context=True,
    dag=dag,
)

# Task 종속성 설정
download_task >> generate_wordcloud_task >> upload_to_s3_task
