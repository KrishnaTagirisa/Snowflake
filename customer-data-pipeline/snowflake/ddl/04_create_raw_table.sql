-- =============================================================================
-- 04_create_raw_table.sql
-- The RAW landing table. Columns are kept close to the source (light typing
-- only) plus a few pipeline metadata columns for lineage/debugging -- a
-- common "raw vault" style pattern. Real cleaning/typing happens in dbt.
-- =============================================================================

USE DATABASE ANALYTICS;
USE SCHEMA RAW;

CREATE TABLE IF NOT EXISTS RAW.CUSTOMERS_RAW (
    customer_id       VARCHAR,                                   -- kept as text at raw layer; cast in staging
    name              VARCHAR,
    country            VARCHAR,
    dob               VARCHAR,                                   -- kept as text; cast to DATE in staging
    _file_name        VARCHAR DEFAULT METADATA$FILENAME,          -- Snowflake COPY INTO metadata: source file
    _file_row_number  NUMBER  DEFAULT METADATA$FILE_ROW_NUMBER,   -- row position within the source file
    _loaded_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),  -- when Snowflake ingested the row
    _source_system    VARCHAR DEFAULT 'S3_CUSTOMER_FEED'
)
COMMENT = 'Raw, minimally-typed landing table for customer records loaded from S3 by Airflow';
