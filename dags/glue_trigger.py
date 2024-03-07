from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "glue_job_trigger",
    default_args=default_args,
    description="Trigger an AWS Glue job from Airflow and wait for completion",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

# 첫 번째 Glue 작업 실행
trigger_glue_job_movie = GlueJobOperator(
    task_id="trigger_glue_job_movie",
    job_name="de-4-2-movie",
    script_location="s3://aws-glue-assets-862327261051-ap-northeast-2/scripts/de-4-2-movie.py",
    aws_conn_id="aws_conn",
    region_name="ap-northeast-2",
    dag=dag,
#    retries=3,  # 재시도 횟수
#    retry_delay=timedelta(minutes=3),  # 재시도 간격
)

# 첫 번째 Glue 작업 완료 대기
#wait_for_glue_job_movie = GlueJobSensor(
#    task_id="wait_for_glue_job_movie",
#    job_name="de-4-2-movie",
#    aws_conn_id="aws_conn",
    #region_name="ap-northeast-2",
#    run_id="{{ task_instance.xcom_pull(task_ids='trigger_glue_job_movie', key='return_value') }}",
#    dag=dag,
#)

# 두 번째 Glue 작업 실행
#trigger_glue_job_directors = GlueJobOperator(
#    task_id="trigger_glue_job_directors",
#    job_name="de-4-2-kofic-movie-directors",
#    script_location="s3://aws-glue-assets-862327261051-ap-northeast-2/scripts/de-4-2-kofic-movie-directors.py",
#    aws_conn_id="aws_conn",
#    region_name="ap-northeast-2",
#    dag=dag,
#)

# 작업 흐름 정의
#trigger_glue_job_movie >> wait_for_glue_job_movie >> trigger_glue_job_directors
