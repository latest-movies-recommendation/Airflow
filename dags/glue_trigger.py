# IAM: EC2 인스턴스에 AWSGlueConsoleFullAccess, S3FullAccess 부여 필수
# 수정해야 함.
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'glue_job_trigger',
    default_args=default_args,
    description='Trigger an AWS Glue job from Airflow',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

trigger_glue_job = GlueJobOperator(
    task_id='trigger_glue_job',
    job_name='S3-test-job',   # AWS Glue에서 생성한 Job 이름으로 교체해야 합니다.
    script_location='s3://aws-glue-assets-654654507336-ap-northeast-2/scripts/your_script_name.py', # 실제 스크립트 파일 이름으로 교체해야 합니다.
    aws_conn_id='aws_conn',
    region_name='ap-northeast-2',
    dag=dag,
)

trigger_glue_job
