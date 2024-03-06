from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="HelloWorld",
    start_date=datetime(2022, 5, 5),
    catchup=False,
    tags=["example"],
    schedule="0 2 * * *",
)


def print_hello():
    print("hello!")
    return "hello!"


def print_goodbye():
    print("goodbye!")
    return "goodbye!"


print_hello = PythonOperator(
    task_id="print_hello", python_callable=print_hello, dag=dag
)

print_goodbye = PythonOperator(
    task_id="print_goodbye", python_callable=print_goodbye, dag=dag
)


print_hello >> print_goodbye
