from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 5, 25),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'binance_ml_train_every_hour',
    default_args=default_args,
    description='Train ML for Binance Futures mỗi 1h',
    schedule_interval='@hourly',
    catchup=False,
)

# Crawl data và lưu csv
crawl_task = BashOperator(
    task_id='crawl_binance_data',
    bash_command='python /home/airflow/gcs/data/scripts/train_binance.py',
    dag=dag,
)

# Train model và lưu model/report (lệnh dưới sẽ tự lấy timestamp bên trong script)
train_task = BashOperator(
    task_id='train_rf_model',
    bash_command='python /home/airflow/gcs/data/scripts/train_model.py',
    dag=dag,
)

crawl_task >> train_task
