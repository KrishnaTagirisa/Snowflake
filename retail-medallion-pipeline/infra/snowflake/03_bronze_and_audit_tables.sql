-- =============================================================================
-- 03_bronze_and_audit_tables.sql
-- Bronze = raw, append-only, schema-on-read. Everything is VARCHAR or VARIANT
-- so a malformed value can never fail the load; typing happens in SILVER.
-- =============================================================================
USE ROLE RETAIL_LOADER;
USE DATABASE RETAIL_PROD;

-- -----------------------------------------------------------------------------
-- BRONZE tables
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS BRONZE.CUSTOMERS_RAW (
    customer_id      VARCHAR,
    first_name       VARCHAR,
    last_name        VARCHAR,
    email            VARCHAR,
    phone            VARCHAR,
    city             VARCHAR,
    country          VARCHAR,
    segment          VARCHAR,
    created_at       VARCHAR,
    updated_at       VARCHAR,
    -- lineage / audit
    _source_file     VARCHAR,
    _source_row      NUMBER,
    _file_last_modified TIMESTAMP_LTZ,
    _loaded_at       TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    _batch_id        VARCHAR
)
COMMENT = 'Raw customers daily snapshot (CSV)';

CREATE TABLE IF NOT EXISTS BRONZE.PRODUCTS_RAW (
    product_id       VARCHAR,
    product_name     VARCHAR,
    category         VARCHAR,
    sub_category     VARCHAR,
    brand            VARCHAR,
    unit_price       VARCHAR,
    currency         VARCHAR,
    is_active        VARCHAR,
    updated_at       VARCHAR,
    _source_file     VARCHAR,
    _source_row      NUMBER,
    _file_last_modified TIMESTAMP_LTZ,
    _loaded_at       TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    _batch_id        VARCHAR
)
COMMENT = 'Raw products daily snapshot (CSV)';

CREATE TABLE IF NOT EXISTS BRONZE.ORDERS_RAW (
    payload          VARIANT,
    _source_file     VARCHAR,
    _source_row      NUMBER,
    _file_last_modified TIMESTAMP_LTZ,
    _loaded_at       TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    _batch_id        VARCHAR
)
COMMENT = 'Raw orders (NDJSON, nested items[])';

-- -----------------------------------------------------------------------------
-- AUDIT: one row per file per COPY execution (written by the Airflow DAG)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS AUDIT.LOAD_LOG (
    batch_id         VARCHAR,
    dag_run_id       VARCHAR,
    entity           VARCHAR,
    target_table     VARCHAR,
    load_date        DATE,
    file_name        VARCHAR,
    status           VARCHAR,     -- LOADED / LOAD_FAILED / PARTIALLY_LOADED / LOAD_SKIPPED
    rows_parsed      NUMBER,
    rows_loaded      NUMBER,
    errors_seen      NUMBER,
    first_error      VARCHAR,
    logged_at        TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);
