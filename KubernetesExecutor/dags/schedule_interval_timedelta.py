from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def say_hello():
    print("Hello from a timedelta DAG!")

with DAG(
    dag_id="timedelta",
    description="DAG chạy mỗi 6 giờ dùng timedelta",
    start_date=datetime(2024, 1, 1),
    schedule_interval=timedelta(hours=6),  # Lặp lại mỗi 6 giờ
    catchup=False,
    tags=["timedelta", "example"],
) as dag:

    hello_task = PythonOperator(
        task_id="say_hello_task",
        python_callable=say_hello,
    )

    hello_task
