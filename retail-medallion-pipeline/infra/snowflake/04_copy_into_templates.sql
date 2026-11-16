-- =============================================================================
-- 04_copy_into_templates.sql
-- The exact COPY statements the Airflow DAG runs (shown here for manual runs /
-- backfills). {{ load_date }} and {{ batch_id }} are filled in by Airflow.
--
-- Idempotency: Snowflake keeps 64 days of load metadata per table, so files that
-- were already loaded are skipped automatically. FORCE = FALSE (default) keeps it so.
-- =============================================================================
USE ROLE RETAIL_LOADER;
USE WAREHOUSE LOAD_WH;
USE DATABASE RETAIL_PROD;

-- Customers (CSV) ------------------------------------------------------------
COPY INTO BRONZE.CUSTOMERS_RAW (
    customer_id, first_name, last_name, email, phone, city, country, segment,
    created_at, updated_at,
    _source_file, _source_row, _file_last_modified, _batch_id
)
FROM (
    SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
           METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$FILE_LAST_MODIFIED,
           '{{ batch_id }}'
    FROM @BRONZE.STG_RETAIL_LANDING/customers/load_date={{ load_date }}/
)
FILE_FORMAT = (FORMAT_NAME = 'BRONZE.FF_CSV')
PATTERN = '.*[.]csv([.]gz)?'
ON_ERROR = 'ABORT_STATEMENT';

-- Products (CSV) -------------------------------------------------------------
COPY INTO BRONZE.PRODUCTS_RAW (
    product_id, product_name, category, sub_category, brand, unit_price, currency,
    is_active, updated_at,
    _source_file, _source_row, _file_last_modified, _batch_id
)
FROM (
    SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9,
           METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$FILE_LAST_MODIFIED,
           '{{ batch_id }}'
    FROM @BRONZE.STG_RETAIL_LANDING/products/load_date={{ load_date }}/
)
FILE_FORMAT = (FORMAT_NAME = 'BRONZE.FF_CSV')
PATTERN = '.*[.]csv([.]gz)?'
ON_ERROR = 'ABORT_STATEMENT';

-- Orders (NDJSON) ------------------------------------------------------------
COPY INTO BRONZE.ORDERS_RAW (payload, _source_file, _source_row, _file_last_modified, _batch_id)
FROM (
    SELECT $1,
           METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$FILE_LAST_MODIFIED,
           '{{ batch_id }}'
    FROM @BRONZE.STG_RETAIL_LANDING/orders/load_date={{ load_date }}/
)
FILE_FORMAT = (FORMAT_NAME = 'BRONZE.FF_JSON')
PATTERN = '.*[.]json([.]gz)?'
ON_ERROR = 'ABORT_STATEMENT';

-- Dry-run validation before a risky backfill:
-- COPY INTO BRONZE.CUSTOMERS_RAW ... VALIDATION_MODE = 'RETURN_ERRORS';

-- Investigate history:
-- SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
--     TABLE_NAME => 'BRONZE.ORDERS_RAW', START_TIME => DATEADD(day, -7, CURRENT_TIMESTAMP())));
