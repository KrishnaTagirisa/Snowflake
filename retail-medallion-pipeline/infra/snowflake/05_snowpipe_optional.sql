-- =============================================================================
-- 05_snowpipe_optional.sql
-- ALTERNATIVE to Airflow-driven COPY for feeds that arrive throughout the day.
-- S3 event -> SQS (owned by Snowflake) -> Snowpipe -> BRONZE within ~1 minute.
-- Airflow then only orchestrates dbt (it can check SYSTEM$PIPE_STATUS first).
-- =============================================================================
USE ROLE RETAIL_LOADER;
USE DATABASE RETAIL_PROD;

CREATE PIPE IF NOT EXISTS BRONZE.PIPE_ORDERS_RAW
  AUTO_INGEST = TRUE
  COMMENT = 'Near-real-time orders ingestion'
AS
COPY INTO BRONZE.ORDERS_RAW (payload, _source_file, _source_row, _file_last_modified, _batch_id)
FROM (
    SELECT $1, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$FILE_LAST_MODIFIED, 'snowpipe'
    FROM @BRONZE.STG_RETAIL_LANDING/orders/
)
FILE_FORMAT = (FORMAT_NAME = 'BRONZE.FF_JSON')
PATTERN = '.*[.]json([.]gz)?'
ON_ERROR = 'SKIP_FILE';

-- Take the notification_channel (SQS ARN) from here and add it as an
-- s3:ObjectCreated:* event notification on prefix retail/orders/ in the bucket.
SHOW PIPES LIKE 'PIPE_ORDERS_RAW' IN SCHEMA BRONZE;

-- Health / troubleshooting
-- SELECT SYSTEM$PIPE_STATUS('BRONZE.PIPE_ORDERS_RAW');
-- ALTER PIPE BRONZE.PIPE_ORDERS_RAW REFRESH;   -- backfill files from the last 7 days
