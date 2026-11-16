-- =============================================================================
-- 05_create_audit_table.sql
-- Operational metadata table the Airflow DAG writes to after every load, so
-- pipeline runs are auditable directly in Snowflake (row counts, status,
-- timings) without needing to dig through Airflow logs.
-- =============================================================================

USE DATABASE ANALYTICS;
CREATE SCHEMA IF NOT EXISTS OPS
    COMMENT = 'Operational/audit metadata for pipeline runs';

CREATE TABLE IF NOT EXISTS OPS.PIPELINE_LOAD_AUDIT (
    run_id           VARCHAR,        -- Airflow run_id, ties back to the DAG run
    dag_id           VARCHAR,
    task_id          VARCHAR,
    source_file      VARCHAR,        -- S3 key that was loaded
    rows_loaded      NUMBER,
    status           VARCHAR,        -- SUCCESS | FAILED
    error_message    VARCHAR,
    started_at       TIMESTAMP_NTZ,
    completed_at     TIMESTAMP_NTZ,
    logged_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Audit log of every Airflow load into RAW.CUSTOMERS_RAW';
