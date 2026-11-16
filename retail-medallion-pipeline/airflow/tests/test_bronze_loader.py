"""Unit tests for the bronze loader - run with: pytest airflow/tests -q"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

from retail.bronze_loader import (  # noqa: E402
    ENTITIES,
    BronzeLoadError,
    build_copy_sql,
    evaluate_copy_results,
    load_entity,
    make_batch_id,
    normalise_copy_results,
)

COPY_DESC = [("file",), ("status",), ("rows_parsed",), ("rows_loaded",), ("error_limit",),
             ("errors_seen",), ("first_error",)]


def test_copy_sql_targets_partition_and_audit_columns():
    sql = build_copy_sql(ENTITIES["customers"], "2026-11-11", "b1")
    assert "@BRONZE.STG_RETAIL_LANDING/customers/load_date=2026-11-11/" in sql
    assert "METADATA$FILENAME" in sql and "_batch_id" in sql
    assert sql.count("$") >= 10
    assert "ON_ERROR = 'ABORT_STATEMENT'" in sql


def test_copy_sql_json_uses_single_variant_column():
    sql = build_copy_sql(ENTITIES["orders"], "2026-11-11", "b1")
    assert "COPY INTO BRONZE.ORDERS_RAW (payload, _source_file" in sql
    assert "SELECT $1," in sql


@pytest.mark.parametrize("bad", ["2026-13-01", "2026-11-11'; DROP TABLE x;--", ""])
def test_rejects_invalid_load_date(bad):
    with pytest.raises(ValueError):
        build_copy_sql(ENTITIES["orders"], bad, "b1")


def test_batch_id_is_escaped_in_sql():
    sql = build_copy_sql(ENTITIES["orders"], "2026-11-11", "o'brien")
    assert "'o''brien'" in sql


def test_zero_files_response_is_empty():
    assert normalise_copy_results([("status",)], [("Copy executed with 0 files processed.",)]) == []


def test_evaluate_ok_and_skipped():
    recs = [{"file": "a", "status": "LOADED", "rows_loaded": 10},
            {"file": "b", "status": "LOAD_SKIPPED", "rows_loaded": 0}]
    assert evaluate_copy_results("x", recs) == {"entity": "x", "files": 2, "rows_loaded": 10}


def test_evaluate_raises_on_partial_load():
    with pytest.raises(BronzeLoadError, match="PARTIALLY_LOADED"):
        evaluate_copy_results("x", [{"file": "a", "status": "PARTIALLY_LOADED", "first_error": "bad"}])


def test_make_batch_id_sanitises():
    assert make_batch_id("scheduled__2026-11-12T06:30:00+00:00", "2026-11-11") == \
        "2026-11-11__scheduled__2026-11-12T06:30:00+00:00"
    assert " " not in make_batch_id("manual run 1", "2026-11-11")


# --- load_entity with a fake Snowflake connection -----------------------------
class FakeCursor:
    def __init__(self, copy_desc, copy_rows, existing_rows=0, copy_error=None):
        self.copy_desc, self.copy_rows = copy_desc, copy_rows
        self.existing_rows, self.copy_error = existing_rows, copy_error
        self.executed, self.description, self._rows = [], None, []

    def execute(self, sql, params=None):
        self.executed.append((sql.strip().split()[0].upper(), params))
        if sql.strip().startswith("COPY INTO"):
            if self.copy_error:
                raise self.copy_error
            self.description, self._rows = self.copy_desc, self.copy_rows
        elif sql.strip().startswith("SELECT COUNT"):
            self._rows = [(self.existing_rows,)]

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0]


class FakeConn:
    def __init__(self, cur):
        self.cur, self.closed = cur, False

    def cursor(self):
        return self.cur

    def commit(self):
        pass

    def close(self):
        self.closed = True


class FakeHook:
    def __init__(self, cur):
        self.conn = FakeConn(cur)

    def get_conn(self):
        return self.conn


def test_load_entity_writes_audit_rows():
    cur = FakeCursor(COPY_DESC, [("s3://f1.csv", "LOADED", 5, 5, 1, 0, None)])
    hook = FakeHook(cur)
    result = load_entity(hook, "customers", "2026-11-11", "b1", "run1")
    assert result["rows_loaded"] == 5
    assert [e[0] for e in cur.executed].count("INSERT") == 1
    assert hook.conn.closed


def test_load_entity_rerun_is_idempotent():
    cur = FakeCursor([("status",)], [("Copy executed with 0 files processed.",)], existing_rows=42)
    assert load_entity(FakeHook(cur), "orders", "2026-11-11", "b2", "run2")["skipped"] is True


def test_load_entity_fails_when_partition_missing():
    cur = FakeCursor([("status",)], [("Copy executed with 0 files processed.",)], existing_rows=0)
    with pytest.raises(BronzeLoadError, match="no files found"):
        load_entity(FakeHook(cur), "orders", "2026-11-11", "b3", "run3")


def test_load_entity_audits_aborted_copy():
    cur = FakeCursor(COPY_DESC, [], copy_error=RuntimeError("Numeric value 'abc' is not recognized"))
    with pytest.raises(BronzeLoadError, match="COPY aborted"):
        load_entity(FakeHook(cur), "products", "2026-11-11", "b4", "run4")
    inserts = [p for verb, p in cur.executed if verb == "INSERT"]
    assert inserts and "not recognized" in inserts[0][-1]
