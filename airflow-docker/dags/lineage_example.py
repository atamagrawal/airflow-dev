from airflow import DAG
from airflow.datasets import Dataset
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

input_ds = Dataset("file:///tmp/input.txt")
output_ds = Dataset("file:///tmp/output.txt")

with DAG(
    dag_id="lineage_smoke_test",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
):

    produce = BashOperator(
        task_id="produce",
        bash_command="echo 'hello lineage' > /tmp/input.txt",
        outlets=[input_ds],
    )

    consume = BashOperator(
        task_id="consume",
        bash_command="cat /tmp/input.txt > /tmp/output.txt",
        inlets=[input_ds],
        outlets=[output_ds],
    )

    produce >> consume
