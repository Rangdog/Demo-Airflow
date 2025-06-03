from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def say_hello():
    print("Hello from Celery Worker!")

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='hello_celery',
    default_args=default_args,
    start_date=datetime(2023, 1, 1),  # đảm bảo DAG có thể chạy ngay
    schedule_interval='@daily',
    catchup=False,
    tags=['demo'],
) as dag:

    task = PythonOperator(
        task_id='say_hello',
        python_callable=say_hello,
    )
