import pendulum
from airflow.models.dag import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup

# DAG 정의
with DAG(
    dag_id="example_task_group",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
) as dag:
    start = DummyOperator(task_id="start")

    # kofic_etl Task Group
    with TaskGroup("kofic_etl", tooltip="코픽 ETL 작업 그룹") as kofic_etl_group:
        get_daily_box_office = DummyOperator(task_id="get_daily_box_office", dag=dag)
        store_movie_codes_to_fetch = DummyOperator(
            task_id="store_movie_codes_to_fetch", dag=dag
        )
        load_movie_codes_to_fetch = DummyOperator(
            task_id="load_movie_codes_to_fetch", dag=dag
        )
        get_movie = DummyOperator(task_id="get_movie", dag=dag)

        (
            get_daily_box_office
            >> store_movie_codes_to_fetch
            >> load_movie_codes_to_fetch
            >> get_movie
        )

    # naver_crawler Task Group
    with TaskGroup(
        "naver_crawler", tooltip="네이버 크롤러 작업 그룹"
    ) as naver_crawler_group:
        trigger_naver = DummyOperator(task_id="trigger_naver", dag=dag)
        s3_to_rank_movie_list = DummyOperator(task_id="s3_to_rank_movie_list", dag=dag)
        s3_to_naver_movie_list = DummyOperator(
            task_id="s3_to_naver_movie_list", dag=dag
        )
        naver_info_crawling = DummyOperator(task_id="naver_info_crawling", dag=dag)
        s3_to_postgres = DummyOperator(task_id="s3_to_postgres", dag=dag)
        critic_review_crawling = DummyOperator(
            task_id="critic_review_crawling", dag=dag
        )
        naver_review_crawling = DummyOperator(task_id="naver_review_crawling", dag=dag)

        (
            trigger_naver
            >> s3_to_rank_movie_list
            >> s3_to_naver_movie_list
            >> naver_info_crawling
            >> s3_to_postgres
            >> critic_review_crawling
            >> naver_review_crawling
        )

    # daily_movie_ratings Task Group
    with TaskGroup(
        "daily_movie_ratings", tooltip="일일 영화 평점 작업 그룹"
    ) as daily_movie_ratings_group:
        get_naver_ratings = DummyOperator(task_id="get_naver_ratings", dag=dag)
        get_watcha_ratings = DummyOperator(task_id="get_watcha_ratings", dag=dag)
        merge_ratings = DummyOperator(task_id="merge_ratings", dag=dag)
        upload_to_postgres = DummyOperator(task_id="upload_to_postgres", dag=dag)

        get_naver_ratings >> get_watcha_ratings >> merge_ratings >> upload_to_postgres

    # glue_job_trigger Task Group
    with TaskGroup(
        "glue_job_trigger", tooltip="Glue Job 트리거 작업 그룹"
    ) as glue_job_trigger_group:
        trigger_glue_job = DummyOperator(task_id="trigger_glue_job", dag=dag)

    end = DummyOperator(task_id="end", dag=dag)

    # Task Group 간의 순서 정의
    (
        start
        >> [
            kofic_etl_group,
            naver_crawler_group,
            daily_movie_ratings_group,
            glue_job_trigger_group,
        ]
        >> end
    )
