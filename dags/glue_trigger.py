from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor


def check_glue_job_status(**kwargs):
    ti = kwargs["ti"]
    # XCom을 사용하여 첫 번째 작업의 상태를 확인합니다.
    glue_job_status = ti.xcom_pull(
        task_ids="trigger_glue_job_movie", key="return_value"
    )

    # 여기서는 단순화를 위해 성공을 가정하고 있으나, 실제 로직에서는 glue_job_status를 검사하여 결정해야 합니다.
    if glue_job_status == "Success" or "Failed":
        return "trigger_glue_job_directors"
    else:
        return "wait_before_retrying"


# 1분 대기하는 Python 함수
def wait_one_minute():
    import time

    time.sleep(60)


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


# 조건부 분기 작업
branching = BranchPythonOperator(
    task_id="branching",
    python_callable=check_glue_job_status,
    trigger_rule="all_done",
    dag=dag,
)

# 실패 시 1분을 기다립니다.
wait_before_retrying = PythonOperator(
    task_id="wait_before_retrying",
    python_callable=wait_one_minute,
    dag=dag,
)

# 두 번째 Glue 작업 실행
trigger_glue_job_directors = GlueJobOperator(
    task_id="trigger_glue_job_directors",
    job_name="de-4-2-kofic-movie-directors",
    script_location="s3://aws-glue-assets-862327261051-ap-northeast-2/scripts/de-4-2-kofic-movie-directors.py",
    aws_conn_id="aws_conn",
    region_name="ap-northeast-2",
    dag=dag,
)

# 더미 작업 정의 (실제 로직에 따라 변경 가능)
trigger_glue_job_directors_dummy = DummyOperator(
    task_id="trigger_glue_job_directors_dummy",
    dag=dag,
)
# 작업 흐름 정의
(
    trigger_glue_job_movie
    >> branching
    >> [wait_before_retrying, trigger_glue_job_directors_dummy]
)
wait_before_retrying >> trigger_glue_job_directors
trigger_glue_job_directors_dummy >> trigger_glue_job_directors
