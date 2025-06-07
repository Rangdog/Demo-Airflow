from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import BaseOperator
from airflow.utils.context import Context
from datetime import datetime, timedelta


# ✅ Custom Operator
class GreetOperator(BaseOperator):
    def __init__(self, name: str, **kwargs):
        super().__init__(**kwargs)
        self.name = name

    def execute(self, context: Context):
        print(f"👋 Hello, {self.name}! This is a custom operator.")


# ✅ Python callable
def say_hello():
    print("👋 Hello from PythonOperator")


# ✅ DAG
with DAG(
    dag_id="full_operator_demo",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["operator", "demo"],
    default_args={
        "email": ["your_email@example.com"],
        "email_on_failure": False,
        "retries": 1,
        "retry_delay": timedelta(seconds=5),
    },
) as dag:

    start = EmptyOperator(task_id="start")

    bash = BashOperator(
        task_id="bash_task",
        bash_command='echo "🖥️ Hello from BashOperator"',
    )

    python = PythonOperator(
        task_id="python_task",
        python_callable=say_hello,
    )

    email = EmailOperator(
        task_id="email_task",
        to="your_email@example.com",
        subject="📧 Airflow EmailOperator Test",
        html_content="<p>Hello from EmailOperator!</p>",
    )

    postgres = PostgresOperator(
        task_id="postgres_task",
        postgres_conn_id="your_postgres_conn_id",  # setup trong Admin > Connections
        sql="SELECT NOW();",  # câu SQL đơn giản để test
    )

    custom = GreetOperator(
        task_id="custom_greet_task",
        name="Airflow User",
    )

    start >> [bash, python, postgres] >> custom >> email
