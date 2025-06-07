from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Hàm Python đơn giản cho task
def say_hello():
    print("Hello from Airflow!")

# Khai báo DAG
with DAG(
    dag_id="preset",
    description="DAG chạy mỗi ngày lúc 00:00",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",  # Cron: 8h sáng Thứ Hai
    catchup=False,  
    tags=["example", "preset"],
) as dag:

    # Định nghĩa task
    hello_task = PythonOperator(
        task_id="say_hello_task",
        python_callable=say_hello,
    )

    hello_task  #