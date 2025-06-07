from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Hàm Python đơn giản cho task
def say_hello():
    print("Hello from Airflow!")

# Khai báo DAG
with DAG(
    dag_id="cron",
    description="DAG chạy vào 8h sáng Thứ Hai hàng tuần",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 8 * * 1",  # Cron: 8h sáng Thứ Hai
    catchup=False, 
    tags=["example", "cron"],
) as dag:

    # Định nghĩa task
    hello_task = PythonOperator(
        task_id="say_hello_task",
        python_callable=say_hello,
    )

    hello_task 

