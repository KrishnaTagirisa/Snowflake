-- =============================================================================
-- 02_create_file_format.sql
-- Defines how Snowflake should parse the incoming CSV files from S3.
-- Centralizing this as a named file format keeps COPY INTO statements
-- (in the Airflow DAG) simple and consistent.
-- =============================================================================

USE DATABASE ANALYTICS;

CREATE SCHEMA IF NOT EXISTS RAW
    COMMENT = 'Landing zone for untransformed data loaded from S3';

CREATE FILE FORMAT IF NOT EXISTS RAW.CUSTOMER_CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('', 'NULL', 'null')
    EMPTY_FIELD_AS_NULL = TRUE
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
    COMMENT = 'CSV format for the customers source file (id, name, country, dob)';
