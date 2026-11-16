"""
retail_medallion_daily
======================
S3 landing -> Snowflake BRONZE (COPY INTO) -> SILVER (dbt) -> GOLD (dbt)

  resolve_run_context
        |
  wait_for_success_marker       (deferrable S3 sensor on retail/_control/load_date=X/_SUCCESS)
        |
  load_bronze[customers|products|orders]   (dynamic task mapping, run in parallel)
        |
  silver   (Cosmos task group: bronze source freshness/tests, then one Airflow task
            per dbt model/snapshot followed by its tests)
        |
  gold     (Cosmos task group)
        |
  publish_run_summary

Which day is loaded:
  * scheduled run at 06:30 London -> previous calendar day's extract
  * manual run / backfill         -> trigger with conf {"load_date": "2026-11-01"}

Airflow connections (store in AWS Secrets Manager secrets backend):
  aws_default            - IAM role for the S3 sensor
  snowflake_default      - SVC_AIRFLOW, key-pair auth, account/database in extras
  slack_data_alerts      - Slack incoming webhook (optional)
"""
from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path

import pendulum

try:  # Airflow 3
    from airflow.sdk import DAG, Param, get_current_context, task
except ImportError:  # Airflow 2.x
    from airflow.decorators import task
    from airflow.models.param import Param
    from airflow.operators.python import get_current_context

    from airflow import DAG

from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.timetables.trigger import CronTriggerTimetable
from cosmos import (
    DbtTaskGroup,
    ExecutionConfig,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.constants import LoadMode, SourceRenderingBehavior, TestBehavior
from cosmos.profiles import SnowflakeEncryptedPrivateKeyPemProfileMapping

from retail.bronze_loader import ENTITIES, load_entity, make_batch_id

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
LOCAL_TZ = pendulum.timezone("Europe/London")
LANDING_BUCKET = os.getenv("RETAIL_LANDING_BUCKET", "acme-retail-landing")
SNOWFLAKE_CONN_ID = "snowflake_default"
AWS_CONN_ID = "aws_default"
SLACK_CONN_ID = "slack_data_alerts"
DBT_TARGET = os.getenv("DBT_TARGET", "prod")

DBT_PROJECT_DIR = Path(os.getenv("DBT_PROJECT_DIR", "/usr/local/airflow/dbt/retail_medallion"))
DBT_EXECUTABLE = os.getenv("DBT_EXECUTABLE", "/usr/local/airflow/dbt_venv/bin/dbt")
DBT_MANIFEST = DBT_PROJECT_DIR / "target" / "manifest.json"   # built in CI by `dbt parse`

RUN_LOAD_DATE = "{{ ti.xcom_pull(task_ids='resolve_run_context')['load_date'] }}"


# ---------------------------------------------------------------------------
# Alerting
# ---------------------------------------------------------------------------
def notify_failure(context) -> None:
    ti = context["task_instance"]
    message = (
        f":red_circle: *{ti.dag_id}* failed\n"
        f"task: `{ti.task_id}`  try: {ti.try_number}\n"
        f"run: `{context['run_id']}`\n"
        f"log: {ti.log_url}"
    )
    try:
        from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

        SlackWebhookHook(slack_webhook_conn_id=SLACK_CONN_ID).send(text=message)
    except Exception as exc:  # alerting must never mask the original failure
        print(f"Slack alert not sent ({exc}). Message was:\n{message}")


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": notify_failure,
}

# ---------------------------------------------------------------------------
# dbt / Cosmos
# ---------------------------------------------------------------------------
profile_config = ProfileConfig(
    profile_name="retail_medallion",
    target_name=DBT_TARGET,
    # Credentials come from the Airflow connection, so there is one source of truth.
    profile_mapping=SnowflakeEncryptedPrivateKeyPemProfileMapping(
        conn_id=SNOWFLAKE_CONN_ID,
        profile_args={
            "role": "RETAIL_TRANSFORMER",
            "warehouse": "TRANSFORM_WH",
            "database": "RETAIL_PROD" if DBT_TARGET == "prod" else "RETAIL_DEV",
            "schema": "SILVER",
            "threads": 8,
        },
    ),
)

project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_DIR,
    manifest_path=DBT_MANIFEST,
)

execution_config = ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE)

dbt_operator_args = {
    "install_deps": False,       # packages are installed at image build time
    "full_refresh": "{{ params.full_refresh }}",
}


def render(tag: str, with_sources: bool = False) -> RenderConfig:
    return RenderConfig(
        load_method=LoadMode.DBT_MANIFEST,
        select=[f"tag:{tag}"],
        test_behavior=TestBehavior.AFTER_EACH,   # a model's tests run right after it
        # silver only: render bronze sources as tasks that run `dbt source freshness`
        # and source tests before any model reads them
        source_rendering_behavior=(
            SourceRenderingBehavior.WITH_TESTS_OR_FRESHNESS if with_sources
            else SourceRenderingBehavior.NONE
        ),
    )


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
with DAG(
    dag_id="retail_medallion_daily",
    description="S3 -> Snowflake bronze/silver/gold for retail OMS extracts",
    schedule=CronTriggerTimetable("30 6 * * *", timezone=LOCAL_TZ),
    start_date=pendulum.datetime(2026, 11, 1, tz=LOCAL_TZ),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    dagrun_timeout=timedelta(hours=6),
    params={
        "load_date": Param(None, type=["null", "string"], format="date",
                           description="Override the extract date (YYYY-MM-DD) for backfills"),
        "full_refresh": Param(False, type="boolean",
                              description="Rebuild incremental dbt models from scratch"),
    },
    tags=["retail", "medallion", "snowflake", "dbt"],
    doc_md=__doc__,
) as dag:

    @task
    def resolve_run_context() -> dict:
        ctx = get_current_context()
        override = ctx["params"].get("load_date")
        if override:
            load_date = override
        else:
            run_ts = ctx.get("logical_date") or ctx["dag_run"].run_after
            load_date = run_ts.in_timezone(LOCAL_TZ).subtract(days=1).to_date_string()
        batch_id = make_batch_id(ctx["run_id"], load_date)
        print(f"load_date={load_date} batch_id={batch_id}")
        return {"load_date": load_date, "batch_id": batch_id, "run_id": ctx["run_id"]}

    run_ctx = resolve_run_context()

    wait_for_success_marker = S3KeySensor(
        task_id="wait_for_success_marker",
        aws_conn_id=AWS_CONN_ID,
        bucket_name=LANDING_BUCKET,
        bucket_key=f"retail/_control/load_date={RUN_LOAD_DATE}/_SUCCESS",
        deferrable=True,              # frees the worker slot while waiting
        poke_interval=300,
        timeout=60 * 60 * 4,          # source SLA: files by 10:30
        retries=0,
    )

    @task(max_active_tis_per_dagrun=3, execution_timeout=timedelta(minutes=45))
    def load_bronze(entity: str, run_context: dict) -> dict:
        return load_entity(
            hook=SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID),
            entity_name=entity,
            load_date=run_context["load_date"],
            batch_id=run_context["batch_id"],
            dag_run_id=run_context["run_id"],
        )

    bronze_loads = load_bronze.partial(run_context=run_ctx).expand(entity=list(ENTITIES))

    silver = DbtTaskGroup(
        group_id="silver",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=render("silver", with_sources=True),
        operator_args=dbt_operator_args,
        default_args={"retries": 1},
    )

    gold = DbtTaskGroup(
        group_id="gold",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=render("gold"),
        operator_args=dbt_operator_args,
        default_args={"retries": 1},
    )

    @task(trigger_rule="all_success")
    def publish_run_summary(run_context: dict, loads: list[dict]) -> None:
        hook = SnowflakeHook(
            snowflake_conn_id=SNOWFLAKE_CONN_ID, role="RETAIL_TRANSFORMER", warehouse="TRANSFORM_WH"
        )
        rows = hook.get_records(
            """
            SELECT calendar_date, SUM(orders) AS orders, SUM(net_revenue) AS net_revenue
            FROM GOLD.AGG_DAILY_SALES
            WHERE order_date_key = %(date_key)s
            GROUP BY calendar_date
            """,
            parameters={"date_key": int(run_context["load_date"].replace("-", ""))},
        )
        loaded = ", ".join(f"{ld['entity']}={ld['rows_loaded']}" for ld in loads)
        print(f"Run {run_context['batch_id']} | bronze rows: {loaded} | gold: {rows}")

    summary = publish_run_summary(run_ctx, bronze_loads)

    run_ctx >> wait_for_success_marker >> bronze_loads >> silver >> gold >> summary
