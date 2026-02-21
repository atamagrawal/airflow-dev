from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig
from airflow import DAG
from datetime import datetime

DBT_PROJECT_PATH = "/opt/airflow/dbt_project"

with DAG(
    dag_id="cosmos_dbt_snowflake",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["dbt", "snowflake", "cosmos"],
) as dag:

    dbt_tg = DbtTaskGroup(
        group_id="dbt",
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_PATH,
        ),
        profile_config=ProfileConfig(
            profile_name="snowflake_profile",
            target_name="dev",
            profiles_yml_filepath=f"{DBT_PROJECT_PATH}/profiles.yml",
        ),
    )

