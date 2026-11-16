"""DAG integrity test - catches import errors, cycles and wiring mistakes in CI."""
from pathlib import Path

import pytest

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"


@pytest.fixture(scope="module")
def dagbag():
    from airflow.models import DagBag

    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}, dagbag.import_errors


def test_pipeline_shape(dagbag):
    dag = dagbag.get_dag("retail_medallion_daily")
    assert dag is not None
    assert dag.max_active_runs == 1
    ids = set(dag.task_ids)
    for expected in ("resolve_run_context", "wait_for_success_marker", "load_bronze",
                     "publish_run_summary"):
        assert expected in ids
    assert any(t.startswith("silver.") for t in ids)
    assert any(t.startswith("gold.") for t in ids)

    # gold must run strictly after silver, and silver after bronze loads
    gold_roots = [t for t in dag.tasks if t.task_id.startswith("gold.") and
                  not any(u.task_id.startswith("gold.") for u in t.upstream_list)]
    for t in gold_roots:
        assert any(u.task_id.startswith("silver.") for u in t.upstream_list), t.task_id


def test_every_task_has_retries_and_alerting(dagbag):
    dag = dagbag.get_dag("retail_medallion_daily")
    for t in dag.tasks:
        assert t.on_failure_callback, t.task_id
