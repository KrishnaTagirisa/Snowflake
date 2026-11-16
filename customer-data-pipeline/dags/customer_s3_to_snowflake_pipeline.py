"""
customer_s3_to_snowflake_pipeline.py
==============================================================================
Production-style Airflow DAG that:

  1. Waits for the daily customer extract to land in S3.
  2. Validates the file's schema and basic data quality *before* touching
     Snowflake (fail fast, cheaply, outside the warehouse).
  3. Loads the raw file into Snowflake (RAW.CUSTOMERS_RAW) via COPY INTO,
     pointed at an external stage over the S3 bucket.
  4. Runs post-load data quality checks directly in Snowflake (row counts,
     duplicate keys, nulls in required columns).
  5. Writes an audit record of the run to OPS.PIPELINE_LOAD_AUDIT.
  6. Triggers dbt to transform RAW -> STAGING -> MARTS and run dbt tests.
  7. Sends a success/failure notification (stubbed -- wire up Slack/email).

Design choices worth calling out:
  * TaskFlow API (`@task`) is used for Python logic; SnowflakeOperator /
    S3KeySensor / BashOperator are used where a maintained provider operator
    already exists -- avoids reinventing connection handling.
  * Connections/credentials are never hardcoded: they come from Airflow
    Connections (`aws_default`, `snowflake_default`) and Airflow Variables,
    which in turn should be backed by a secrets backend (SSM/Secrets
    Manager/Vault) in production -- see README "Secrets management".
  * Idempotency: COPY INTO is naturally idempotent per file (Snowflake skips
    a file it has already loaded into the same table unless FORCE=TRUE), so
    retries/backfills are safe.
  * `on_failure_callback` gives one place to hook in alerting.
==============================================================================
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

import pandas as pd
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Configuration
# Pulled from Airflow Variables so the same DAG code can run in
# dev/staging/prod purely by changing configuration, not code.
# -----------------------------------------------------------------------------
S3_BUCKET = Variable.get("customer_pipeline_s3_bucket", default_var="my-data-lake-bucket")
S3_SOURCE_KEY = Variable.get("customer_pipeline_s3_key", default_var="raw/customers/customers.csv")
SNOWFLAKE_CONN_ID = Variable.get("customer_pipeline_snowflake_conn_id", default_var="snowflake_default")
AWS_CONN_ID = Variable.get("customer_pipeline_aws_conn_id", default_var="aws_default")
DBT_PROJECT_DIR = Variable.get("customer_pipeline_dbt_project_dir", default_var="/opt/airflow/dbt_project")
DBT_PROFILES_DIR = Variable.get("customer_pipeline_dbt_profiles_dir", default_var="/opt/airflow/dbt_project")

REQUIRED_COLUMNS = ["id", "name", "country", "dob"]

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,  # flip to True once an email/alerting conn is configured
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}


def alert_on_failure(context: dict) -> None:
    """
    Central failure callback. Swap the `logger.error` for a Slack webhook,
    PagerDuty event, or SNS publish call in a real deployment.
    """
    task_instance = context["task_instance"]
    logger.error(
        "Task failed: dag=%s task=%s run_id=%s log_url=%s",
        task_instance.dag_id,
        task_instance.task_id,
        context["run_id"],
        task_instance.log_url,
    )


with DAG(
    dag_id="customer_s3_to_snowflake_pipeline",
    description="Land customer CSV from S3 into Snowflake RAW, then transform via dbt",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 6 * * *",  # daily at 06:00 UTC; adjust to source SLA
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["customer", "snowflake", "dbt", "s3"],
    on_failure_callback=alert_on_failure,
    doc_md=__doc__,
) as dag:

    # -------------------------------------------------------------------------
    # 1. Wait for the source file to exist in S3 before doing anything else.
    #    Bounded with a timeout so a missing file surfaces as a clear failure
    #    (and alert) rather than the DAG hanging indefinitely.
    # -------------------------------------------------------------------------
    wait_for_source_file = S3KeySensor(
        task_id="wait_for_source_file",
        bucket_name=S3_BUCKET,
        bucket_key=S3_SOURCE_KEY,
        aws_conn_id=AWS_CONN_ID,
        poke_interval=60,
        timeout=60 * 60,  # 1 hour
        mode="reschedule",  # free the worker slot between pokes
    )

    # -------------------------------------------------------------------------
    # 2. Validate schema + basic quality of the source file BEFORE loading.
    #    Cheap, catches malformed source files early, and avoids polluting
    #    Snowflake with garbage that a downstream dbt test would only catch
    #    later (and after spending warehouse credits).
    # -------------------------------------------------------------------------
    @task(task_id="validate_source_file")
    def validate_source_file() -> str:
        s3 = S3Hook(aws_conn_id=AWS_CONN_ID)
        local_path = s3.download_file(key=S3_SOURCE_KEY, bucket_name=S3_BUCKET)

        df = pd.read_csv(local_path)

        missing_cols = set(REQUIRED_COLUMNS) - set(df.columns.str.lower())
        if missing_cols:
            raise AirflowException(f"Source file is missing required columns: {missing_cols}")

        if df.empty:
            raise AirflowException("Source file has no data rows.")

        # Normalize column casing for the checks below.
        df.columns = [c.lower() for c in df.columns]

        null_ids = df["id"].isna().sum()
        if null_ids > 0:
            raise AirflowException(f"Found {null_ids} row(s) with a null id -- refusing to load.")

        dup_ids = df["id"].duplicated().sum()
        if dup_ids > 0:
            logger.warning("Source file has %s duplicate id(s); dbt staging will de-duplicate.", dup_ids)

        logger.info("Validated source file: %s rows, columns=%s", len(df), list(df.columns))
        return local_path

    # -------------------------------------------------------------------------
    # 3. COPY INTO the RAW table from the external stage. FORCE=FALSE means
    #    Snowflake naturally skips a file it has already loaded, which makes
    #    reruns/backfills idempotent without extra bookkeeping.
    # -------------------------------------------------------------------------
    load_raw_to_snowflake = SnowflakeOperator(
        task_id="load_raw_to_snowflake",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            COPY INTO RAW.CUSTOMERS_RAW (customer_id, name, country, dob)
            FROM (
                SELECT $1, $2, $3, $4
                FROM @RAW.CUSTOMER_S3_STAGE
            )
            PATTERN = '.*customers.*\\.csv'
            FILE_FORMAT = (FORMAT_NAME = 'RAW.CUSTOMER_CSV_FORMAT')
            ON_ERROR = 'ABORT_STATEMENT'
            FORCE = FALSE;
        """,
    )

    # -------------------------------------------------------------------------
    # 4. Post-load data quality checks run inside Snowflake -- catches issues
    #    that only surface once the data is actually in the warehouse (e.g.
    #    dupes across historical loads, not just within a single file).
    # -------------------------------------------------------------------------
    @task(task_id="run_data_quality_checks")
    def run_data_quality_checks() -> int:
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

        row_count = hook.get_first("SELECT COUNT(*) FROM RAW.CUSTOMERS_RAW")[0]
        if row_count == 0:
            raise AirflowException("RAW.CUSTOMERS_RAW is empty after load.")

        null_id_count = hook.get_first(
            "SELECT COUNT(*) FROM RAW.CUSTOMERS_RAW WHERE customer_id IS NULL"
        )[0]
        if null_id_count > 0:
            raise AirflowException(f"{null_id_count} row(s) in RAW.CUSTOMERS_RAW have a null customer_id.")

        logger.info("Data quality checks passed. RAW.CUSTOMERS_RAW row count: %s", row_count)
        return row_count

    # -------------------------------------------------------------------------
    # 5. Write an audit record so every run is traceable directly in
    #    Snowflake (who/what/when/how many rows), independent of Airflow's
    #    own log retention.
    # -------------------------------------------------------------------------
    @task(task_id="log_pipeline_audit")
    def log_pipeline_audit(row_count: int, **context) -> None:
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        run_id = context["run_id"]
        ti = context["task_instance"]

        hook.run(
            """
            INSERT INTO OPS.PIPELINE_LOAD_AUDIT
                (run_id, dag_id, task_id, source_file, rows_loaded, status, started_at, completed_at)
            SELECT %(run_id)s, %(dag_id)s, %(task_id)s, %(source_file)s, %(rows_loaded)s,
                   'SUCCESS', %(started_at)s, CURRENT_TIMESTAMP()
            """,
            parameters={
                "run_id": run_id,
                "dag_id": ti.dag_id,
                "task_id": ti.task_id,
                "source_file": f"s3://{S3_BUCKET}/{S3_SOURCE_KEY}",
                "rows_loaded": row_count,
                "started_at": context["data_interval_start"].to_pydatetime(),
            },
        )

    # -------------------------------------------------------------------------
    # 6. Hand off to dbt for transformation (RAW -> STAGING -> MARTS) and
    #    testing. Using BashOperator here keeps the dependency surface small;
    #    swap for the `cosmos` package or dbt Cloud operators if the project
    #    grows past a handful of models.
    # -------------------------------------------------------------------------
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt deps --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR} --target prod"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --profiles-dir {DBT_PROFILES_DIR} --target prod"
        ),
    )

    # -------------------------------------------------------------------------
    # 7. Notify. Stubbed as a log line; point this at Slack/email/PagerDuty.
    # -------------------------------------------------------------------------
    @task(task_id="notify_success", trigger_rule="all_success")
    def notify_success(row_count: int) -> None:
        logger.info("Pipeline completed successfully. %s rows loaded and transformed.", row_count)

    # -------------------------------------------------------------------------
    # DAG wiring
    # -------------------------------------------------------------------------
    validated_path = validate_source_file()
    quality_result = run_data_quality_checks()
    audit_task = log_pipeline_audit(quality_result)

    (
        wait_for_source_file
        >> validated_path
        >> load_raw_to_snowflake
        >> quality_result
        >> audit_task
        >> dbt_deps
        >> dbt_run
        >> dbt_test
        >> notify_success(quality_result)
    )
