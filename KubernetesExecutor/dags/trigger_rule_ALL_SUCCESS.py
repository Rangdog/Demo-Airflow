from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowSkipException
from datetime import datetime


# Các hàm mô phỏng trạng thái task
def task_success():
    print("✅ Task thành công!")

def task_fail():
    raise Exception("❌ Task thất bại!")

def task_skip():
    raise AirflowSkipException("⚠️ Task bị skip!")

def final_task():
    print("🎯 Task cuối cùng chạy tùy theo trigger_rule")


with DAG(
    dag_id="trigger_rule_ALL_SUCCESS",
    description="Demo trigger_rule trong Airflow",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  
    catchup=False,
    tags=["trigger_rule", "demo"]
) as dag:

    t1 = PythonOperator(
        task_id="task_success",
        python_callable=task_success,
    )

    t2 = PythonOperator(
        task_id="task_fail",
        python_callable=task_fail,
    )

    t3 = PythonOperator(
        task_id="task_skip",
        python_callable=task_skip,
    )

    t4 = PythonOperator(
        task_id="final_task_trigger_rule",
        python_callable=final_task,
        trigger_rule=TriggerRule.ALL_SUCCESS,  
    )

    [t1, t2, t3] >> t4
