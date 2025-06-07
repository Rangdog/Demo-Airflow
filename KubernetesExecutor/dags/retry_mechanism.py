from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import random

def unstable_task():
    if random.random() < 0.7:  # 70% khả năng thất bại
        raise Exception("🔁 Task thất bại ngẫu nhiên – sẽ retry")
    print("✅ Task thành công!")

with DAG(
    dag_id="task_retry_demo",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["retry", "demo"]
) as dag:

    retrying_task = PythonOperator(
        task_id="unstable_task",
        python_callable=unstable_task,
        retries=3,  # Số lần thử lại
        retry_delay=timedelta(seconds=10),  # Chờ 10s mỗi lần
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(seconds=60),
    )
