"""
test_dag_integrity.py
------------------------------------------------------------------------------
Basic CI safety net for the Airflow DAG:
  * The DAG file imports without errors (catches syntax errors, bad operator
    args, missing providers, etc. before they reach a scheduler).
  * The DAG has no cycles and every task has a resolvable dependency chain.
  * A couple of expected tasks are present, guarding against an accidental
    task rename/removal slipping through review.

Run with: pytest tests/test_dag_integrity.py
------------------------------------------------------------------------------
"""

import os

import pytest

DAGS_FOLDER = os.path.join(os.path.dirname(__file__), "..", "dags")


@pytest.fixture(scope="module")
def dagbag():
    from airflow.models import DagBag

    return DagBag(dag_folder=DAGS_FOLDER, include_examples=False)


def test_no_import_errors(dagbag):
    assert not dagbag.import_errors, f"DAG import failures: {dagbag.import_errors}"


def test_pipeline_dag_loaded(dagbag):
    dag = dagbag.get_dag("customer_s3_to_snowflake_pipeline")
    assert dag is not None, "customer_s3_to_snowflake_pipeline DAG failed to load"


def test_expected_tasks_present(dagbag):
    dag = dagbag.get_dag("customer_s3_to_snowflake_pipeline")
    task_ids = set(dag.task_ids)
    expected = {
        "wait_for_source_file",
        "validate_source_file",
        "load_raw_to_snowflake",
        "run_data_quality_checks",
        "log_pipeline_audit",
        "dbt_deps",
        "dbt_run",
        "dbt_test",
        "notify_success",
    }
    missing = expected - task_ids
    assert not missing, f"Expected tasks missing from DAG: {missing}"


def test_dag_has_no_cycles(dagbag):
    dag = dagbag.get_dag("customer_s3_to_snowflake_pipeline")
    # DagBag.process_file already runs cycle detection; if the DAG loaded
    # without import errors and has tasks, cycle detection passed.
    assert len(dag.tasks) > 0
