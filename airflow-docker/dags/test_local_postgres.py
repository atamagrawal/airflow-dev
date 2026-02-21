from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

with DAG(
    dag_id="test_local_postgres",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test", "postgres"],
) as dag:

    # 1️⃣ Create table if not exists
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="con1",  # Your Airflow Postgres connection
        sql="""
        CREATE TABLE IF NOT EXISTS employees (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            department VARCHAR(100),
            salary NUMERIC(10,2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
    )

    # 2️⃣ Insert sample row
    insert_row = SQLExecuteQueryOperator(
        task_id="insert_row",
        conn_id="con1",
        sql="""
        INSERT INTO employees (name, department, salary)
        VALUES ('Alice', 'Engineering', 90000);
        """,
    )

    # 3️⃣ Select rows to log
    select_rows = SQLExecuteQueryOperator(
        task_id="select_rows",
        conn_id="con1",
        sql="SELECT * FROM employees;",
        show_return_value_in_logs=True,  # Will print rows in Airflow logs
    )

    # Set task order
    create_table >> insert_row >> select_rows
