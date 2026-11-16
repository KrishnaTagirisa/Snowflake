"""
Bronze ingestion helpers: build COPY INTO statements, run them, audit the results.

Kept free of Airflow imports (apart from the hook passed in) so the logic can be
unit-tested without a scheduler: see airflow/tests/test_bronze_loader.py.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date

log = logging.getLogger(__name__)

STAGE = "BRONZE.STG_RETAIL_LANDING"
AUDIT_TABLE = "AUDIT.LOAD_LOG"
OK_STATUSES = {"LOADED", "LOAD_SKIPPED"}   # LOAD_SKIPPED = already loaded (idempotent rerun)


@dataclass(frozen=True)
class EntityConfig:
    name: str              # S3 prefix under retail/
    table: str             # bronze target table
    file_format: str       # Snowflake named file format
    file_pattern: str      # regex for files inside the partition
    columns: tuple[str, ...]  # business columns in file order ("payload" for JSON)


ENTITIES: dict[str, EntityConfig] = {
    "customers": EntityConfig(
        name="customers",
        table="BRONZE.CUSTOMERS_RAW",
        file_format="BRONZE.FF_CSV",
        file_pattern=r".*[.]csv([.]gz)?",
        columns=("customer_id", "first_name", "last_name", "email", "phone", "city",
                 "country", "segment", "created_at", "updated_at"),
    ),
    "products": EntityConfig(
        name="products",
        table="BRONZE.PRODUCTS_RAW",
        file_format="BRONZE.FF_CSV",
        file_pattern=r".*[.]csv([.]gz)?",
        columns=("product_id", "product_name", "category", "sub_category", "brand",
                 "unit_price", "currency", "is_active", "updated_at"),
    ),
    "orders": EntityConfig(
        name="orders",
        table="BRONZE.ORDERS_RAW",
        file_format="BRONZE.FF_JSON",
        file_pattern=r".*[.]json([.]gz)?",
        columns=("payload",),
    ),
}

AUDIT_COLUMNS = ("_source_file", "_source_row", "_file_last_modified", "_batch_id")


class BronzeLoadError(RuntimeError):
    """Raised when a COPY INTO finishes with rejected files or loads nothing."""


def _validate_load_date(load_date: str) -> str:
    # Also guards against SQL injection: the value is interpolated into a stage path.
    date.fromisoformat(load_date)
    return load_date


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def build_copy_sql(entity: EntityConfig, load_date: str, batch_id: str) -> str:
    load_date = _validate_load_date(load_date)
    target_cols = ", ".join(entity.columns + AUDIT_COLUMNS)
    positional = ", ".join(f"${i}" for i in range(1, len(entity.columns) + 1))
    return f"""
COPY INTO {entity.table} ({target_cols})
FROM (
    SELECT {positional},
           METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$FILE_LAST_MODIFIED,
           {_sql_literal(batch_id)}
    FROM @{STAGE}/{entity.name}/load_date={load_date}/
)
FILE_FORMAT = (FORMAT_NAME = '{entity.file_format}')
PATTERN = {_sql_literal(entity.file_pattern)}
ON_ERROR = 'ABORT_STATEMENT'
""".strip()


def normalise_copy_results(description, rows) -> list[dict]:
    """Turn a cursor result into dicts. Handles Snowflake's 0-files response, which is a
    single 'status' column such as 'Copy executed with 0 files processed.'"""
    cols = [d[0].lower() for d in description]
    records = [dict(zip(cols, r)) for r in rows]
    if cols == ["status"]:
        return []
    return records


def evaluate_copy_results(entity: str, records: list[dict]) -> dict:
    """Raise if any file failed; return a summary for logging/XCom."""
    failed = [r for r in records if str(r.get("status", "")).upper() not in OK_STATUSES]
    if failed:
        detail = "; ".join(f"{r.get('file')}: {r.get('status')} - {r.get('first_error')}" for r in failed)
        raise BronzeLoadError(f"[{entity}] {len(failed)} file(s) failed to load: {detail}")
    return {
        "entity": entity,
        "files": len(records),
        "rows_loaded": sum(int(r.get("rows_loaded") or 0) for r in records),
    }


def load_entity(hook, entity_name: str, load_date: str, batch_id: str, dag_run_id: str,
                role: str = "RETAIL_LOADER", warehouse: str = "LOAD_WH") -> dict:
    """Run COPY INTO for one entity/partition, write AUDIT.LOAD_LOG, fail loudly on errors."""
    entity = ENTITIES[entity_name]
    sql = build_copy_sql(entity, load_date, batch_id)

    conn = hook.get_conn()
    try:
        cur = conn.cursor()
        cur.execute(f"USE ROLE {role}")
        cur.execute(f"USE WAREHOUSE {warehouse}")
        cur.execute(f"ALTER SESSION SET QUERY_TAG = 'airflow:{entity_name}:{load_date}'")

        log.info("Running COPY for %s / %s", entity_name, load_date)
        try:
            cur.execute(sql)
        except Exception as exc:
            # ON_ERROR=ABORT_STATEMENT raises on the first bad file: record it, then fail.
            cur.execute(
                f"""INSERT INTO {AUDIT_TABLE}
                    (batch_id, dag_run_id, entity, target_table, load_date, status, first_error)
                    VALUES (%s, %s, %s, %s, %s, 'LOAD_FAILED', %s)""",
                (batch_id, dag_run_id, entity_name, entity.table, load_date, str(exc)[:1000]),
            )
            conn.commit()
            raise BronzeLoadError(f"[{entity_name}] COPY aborted: {exc}") from exc
        records = normalise_copy_results(cur.description, cur.fetchall())

        if not records:
            # Nothing new was copied. That is fine on a rerun (files already loaded),
            # but not if this partition has never been loaded at all.
            cur.execute(
                f"SELECT COUNT(*) FROM {entity.table} WHERE _source_file LIKE %s",
                (f"%{entity.name}/load_date={load_date}/%",),
            )
            already_loaded = cur.fetchone()[0]
            if already_loaded == 0:
                raise BronzeLoadError(
                    f"[{entity_name}] no files found in s3 partition load_date={load_date}"
                )
            log.info("%s/%s already loaded (%s rows) - skipping", entity_name, load_date, already_loaded)
            return {"entity": entity_name, "files": 0, "rows_loaded": 0, "skipped": True}

        for r in records:
            cur.execute(
                f"""INSERT INTO {AUDIT_TABLE}
                    (batch_id, dag_run_id, entity, target_table, load_date, file_name, status,
                     rows_parsed, rows_loaded, errors_seen, first_error)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (batch_id, dag_run_id, entity_name, entity.table, load_date, r.get("file"),
                 r.get("status"), r.get("rows_parsed"), r.get("rows_loaded"),
                 r.get("errors_seen"), r.get("first_error")),
            )
        conn.commit()
        summary = evaluate_copy_results(entity_name, records)
        log.info("Loaded %s", summary)
        return summary
    finally:
        conn.close()


BATCH_ID_RE = re.compile(r"[^A-Za-z0-9_.:+-]")


def make_batch_id(dag_run_id: str, load_date: str) -> str:
    return f"{load_date}__{BATCH_ID_RE.sub('_', dag_run_id)}"
