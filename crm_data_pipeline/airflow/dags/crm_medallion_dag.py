from datetime import timedelta
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pendulum

DBT_PROJECT_DIR = os.environ.get("CRM_DBT_PROJECT_DIR", "/opt/airflow/dags/crm_medallion/dbt")
DBT_PROFILES_DIR = os.environ.get("CRM_DBT_PROFILES_DIR", "/opt/airflow/.dbt")

def validate_runtime_configuration():
    if not os.path.isdir(DBT_PROJECT_DIR):
        raise RuntimeError(f"dbt project directory does not exist: {DBT_PROJECT_DIR}")
    if not os.path.isfile(os.path.join(DBT_PROJECT_DIR, "dbt_project.yml")):
        raise RuntimeError("dbt_project.yml not found")
    if not os.path.isdir(DBT_PROFILES_DIR):
        raise RuntimeError("dbt profiles directory does not exist")

with DAG(
    dag_id="crm_medallion_pipeline",
    description="Transform CRM Bronze CDC events into Silver and Gold models",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="*/5 * * * *", catchup=False, max_active_runs=1,
    default_args={"owner":"data-platform","depends_on_past":False,"retries":3,
                  "retry_delay":timedelta(minutes=2),"retry_exponential_backoff":True,
                  "max_retry_delay":timedelta(minutes=15)},
    dagrun_timeout=timedelta(minutes=45), tags=["crm","snowflake","dbt","medallion"]
) as dag:
    config_check = PythonOperator(task_id="validate_runtime_configuration", python_callable=validate_runtime_configuration)
    freshness = BashOperator(task_id="dbt_source_freshness",
        bash_command="cd {{ params.project_dir }} && dbt source freshness --profiles-dir {{ params.profiles_dir }}",
        params={"project_dir":DBT_PROJECT_DIR,"profiles_dir":DBT_PROFILES_DIR})
    build = BashOperator(task_id="dbt_build_and_test",
        bash_command="cd {{ params.project_dir }} && dbt build --profiles-dir {{ params.profiles_dir }}",
        params={"project_dir":DBT_PROJECT_DIR,"profiles_dir":DBT_PROFILES_DIR})
    config_check >> freshness >> build
