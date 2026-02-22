from datetime import datetime
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG(
    dag_id='postgres_lineage_simple',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['simple', 'lineage', 'example'],
) as dag:

    # Task 1: Create a source table
    create_orders = SQLExecuteQueryOperator(
        task_id='create_orders',
        conn_id='con1',
        sql="""
            DROP TABLE IF EXISTS orders;
            CREATE TABLE orders (
                order_id INT,
                customer_id INT,
                amount DECIMAL(10,2),
                order_date DATE
            );

            INSERT INTO orders VALUES
                (1, 101, 100.00, '2024-01-01'),
                (2, 102, 200.00, '2024-01-02'),
                (3, 101, 150.00, '2024-01-03');
        """,
    )

    # Task 2: Simple transformation - copy columns
    # Lineage: summary.customer_id <- orders.customer_id
    #          summary.order_count <- orders.order_id (COUNT)
    #          summary.total_amount <- orders.amount (SUM)
    create_summary = SQLExecuteQueryOperator(
        task_id='create_summary',
        conn_id='con1',
        sql="""
            DROP TABLE IF EXISTS customer_summary;

            CREATE TABLE customer_summary AS
            SELECT
                customer_id,
                COUNT(order_id) as order_count,
                SUM(amount) as total_amount
            FROM orders
            GROUP BY customer_id;
        """,
    )

    # Task 3: Read data (even SELECT generates lineage)
    read_summary = SQLExecuteQueryOperator(
        task_id='read_summary',
        conn_id='con1',
        sql="SELECT * FROM customer_summary;",
    )

    # Task 4: Another transformation with CASE statement
    # Lineage: customer_tier.customer_id <- customer_summary.customer_id
    #          customer_tier.tier <- customer_summary.total_amount (CASE transformation)
    create_tier = SQLExecuteQueryOperator(
        task_id='create_tier',
        conn_id='con1',
        sql="""
            DROP TABLE IF EXISTS customer_tier;

            CREATE TABLE customer_tier AS
            SELECT
                customer_id,
                CASE
                    WHEN total_amount > 200 THEN 'Gold'
                    WHEN total_amount > 100 THEN 'Silver'
                    ELSE 'Bronze'
                END as tier
            FROM customer_summary;
        """,
    )

    # Define task flow
    create_orders >> create_summary >> read_summary >> create_tier


