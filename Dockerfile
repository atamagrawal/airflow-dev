FROM apache/airflow:3.1.7-python3.10

USER airflow

RUN pip install --no-cache-dir \
    astronomer-cosmos==1.13.0 \
    dbt-core==1.11.5 \
    dbt-snowflake==1.11.1 \
    snowflake-connector-python>=4.2.0
