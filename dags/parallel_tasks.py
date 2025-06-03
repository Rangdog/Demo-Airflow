from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time
import random

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}

def do_work(task_number):
    sleep_time = random.randint(5, 15)
    print(f"Task {task_number} sleeping for {sleep_time}s")
    time.sleep(sleep_time)
    print(f"Task {task_number} done")

with DAG(
    dag_id='parallel_tasks',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['demo', 'parallel'],
) as dag:

    tasks = []
    for i in range(5):
        t = PythonOperator(
            task_id=f'task_{i}',
            python_callable=do_work,
            op_args=[i],
        )
        tasks.append(t)
