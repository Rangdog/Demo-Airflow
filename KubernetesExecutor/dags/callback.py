from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from datetime import datetime, timedelta

def log_callback(context):
    print(f"📘 Callback Triggered for task: {context['task_instance'].task_id}")
    print(f"🔍 State: {context['task_instance'].state}")
    print(f"📅 Execution time: {context['execution_date']}")

def always_fail():
    raise Exception("❌ Task intentionally failed")

def always_skip():
    raise AirflowSkipException("⚠️ Task skipped intentionally")

def always_succeed():
    print("✅ Task succeeded!")

with DAG(
    dag_id="callback_demo_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["callback", "demo"],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(seconds=10),
        "on_success_callback": log_callback,
        "on_failure_callback": log_callback,
        "on_retry_callback": log_callback,
    }
) as dag:

    task_success = PythonOperator(
        task_id="task_success",
        python_callable=always_succeed,
        on_execute_callback=log_callback,
    )

    task_fail = PythonOperator(
        task_id="task_fail",
        python_callable=always_fail,
        on_execute_callback=log_callback,
    )

    task_skip = PythonOperator(
        task_id="task_skip",
        python_callable=always_skip,
        on_execute_callback=log_callback,
        on_skipped_callback=log_callback,
    )

    [task_success, task_fail, task_skip]
