from airflow import DAG
from airflow.decorators import task
from datetime import datetime

with DAG(
    dag_id="hello_airflow",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    @task
    def hello():
        print("Hello Airflow 👋")

    hello()

