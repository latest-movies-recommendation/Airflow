from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

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
    description="Trigger an AWS Glue job from Airflow",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

trigger_glue_job = GlueJobOperator(
    task_id="trigger_glue_job",
    job_name="de-4-2-movie",  # AWS Glue에서 생성한 Job 이름으로 교체해야 합니다.
    script_location="s3://aws-glue-assets-862327261051-ap-northeast-2/scripts/de-4-2-movie.py",  # 실제 스크립트 파일 이름으로 교체해야 합니다.
    aws_conn_id="aws_conn",
    region_name="ap-northeast-2",
    dag=dag,
)
