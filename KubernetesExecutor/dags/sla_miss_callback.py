from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time

# Callback sẽ gọi khi vi phạm SLA
def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    print("🚨 SLA Miss Callback Triggered!")
    print(f"⚠️ Task(s) missed SLA: {task_list}")
    print(f"🔍 DAG: {dag.dag_id}")
    print(f"⏰ At: {datetime.now()}")

# Task giả lập chạy lâu (ví dụ: 60 giây)
def long_running_task():
    print("⏳ Starting long task...")
    time.sleep(60)  # deliberately exceed SLA
    print("✅ Task finished!")

with DAG(
    dag_id="sla_demo_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    sla_miss_callback=sla_miss_callback,
    tags=["sla", "demo"]
) as dag:

    task_with_sla = PythonOperator(
        task_id="long_task_with_sla",
        python_callable=long_running_task,
        sla=timedelta(seconds=30),  # đặt SLA nhỏ hơn thời gian thực tế
    )
