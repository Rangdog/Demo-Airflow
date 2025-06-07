from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

with DAG(
    dag_id="catchup_backfill_demo",
    start_date=datetime(2025, 6, 1),  # ngày trong quá khứ
    schedule_interval="@daily",       # chạy hàng ngày
    catchup=False,                    # thử đổi thành True để so sánh
    # catchup=True, 
    tags=["demo", "catchup"],
) as dag:
    
    run_me = EmptyOperator(task_id="simple_task")

#airflow dags backfill -s 2025-06-01 -e 2025-06-03 catchup_backfill_demo chạy backfill
