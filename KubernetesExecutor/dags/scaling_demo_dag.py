from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import time

def dummy_task(task_id):
    print(f"Task {task_id} is starting...")
    time.sleep(10)  # Giả lập xử lý tốn thời gian
    print(f"Task {task_id} is done.")

with DAG(
    dag_id="scaling_demo",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=2,       # Tối đa 2 DAG runs song song
    concurrency=4,           # Tối đa 4 task song song trong toàn DAG
    max_active_tasks=3       # Tối đa 3 task chạy song song trong một DAG run
) as dag:

    for i in range(5):
        PythonOperator(
            task_id=f"task_{i}",
            python_callable=dummy_task,
            op_args=[f"task_{i}"],
            max_active_tis_per_dag=1,  # Chỉ 1 instance của task này chạy cùng lúc
            pool='limited_pool'        # Pool chỉ cho 2 task chạy đồng thời
        )
