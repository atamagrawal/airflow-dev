from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

with DAG(
    dag_id="test_snowflake_connection",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test"],
) as dag:

    test_conn = SQLExecuteQueryOperator(
        task_id="test_snowflake",
        conn_id="snowflake",  # your connection ID
        sql="SELECT CURRENT_VERSION()",
    )
