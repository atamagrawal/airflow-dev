from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello():
    print("Hello Airflow!")

with DAG(dag_id="hello_airflow_1", start_date=datetime(2026,1,1), schedule=None, catchup=False) as dag:
    task = PythonOperator(task_id="say_hello", python_callable=hello)

