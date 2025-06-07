from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.operators.python import PythonOperator
from datetime import datetime

def get_posts():
    hook = HttpHook(method='GET', http_conn_id='json_placeholder')
    response = hook.run(endpoint='posts/1')
    print(response.text)

with DAG(
    dag_id='demo_http_hook',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    task = PythonOperator(
        task_id='get_post',
        python_callable=get_posts
    )
