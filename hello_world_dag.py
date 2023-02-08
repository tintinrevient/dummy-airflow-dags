from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

def print_hello_world():
    return "Hello world!"

dag = DAG("hello_world", description="Hello World DAG",
          schedule_interval="0 12 * * *",
          start_date=datetime(2017, 3, 20), catchup=False)

with DAG(
    "hello_world",
    description="Hello World DAG",
    schedule=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False
) as dag:

  hello_world_operator = PythonOperator(task_id='hello_world_task', python_callable=print_hello_world)

  hello_world_operator
