-- =============================================================================
-- 03_create_stage.sql
-- Creates an external stage pointing at the S3 bucket/prefix the source
-- system writes to. Two auth options are shown -- prefer the storage
-- integration (no long-lived keys stored in Snowflake) in production.
-- =============================================================================

USE DATABASE ANALYTICS;
USE SCHEMA RAW;

-- ---------------------------------------------------------------------------
-- OPTION A (recommended for production): STORAGE INTEGRATION
-- Requires ACCOUNTADMIN to create, then delegate USAGE to TRANSFORM_ROLE.
-- Avoids storing AWS keys in Snowflake; trust is established via an IAM role
-- + external ID instead. Uncomment and fill in your IAM role ARN.
-- ---------------------------------------------------------------------------
-- CREATE STORAGE INTEGRATION IF NOT EXISTS S3_CUSTOMER_INTEGRATION
--     TYPE = EXTERNAL_STAGE
--     STORAGE_PROVIDER = 'S3'
--     ENABLED = TRUE
--     STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<account_id>:role/snowflake-s3-access'
--     STORAGE_ALLOWED_LOCATIONS = ('s3://<your-bucket>/raw/customers/');
--
-- GRANT USAGE ON INTEGRATION S3_CUSTOMER_INTEGRATION TO ROLE TRANSFORM_ROLE;
--
-- CREATE STAGE IF NOT EXISTS RAW.CUSTOMER_S3_STAGE
--     STORAGE_INTEGRATION = S3_CUSTOMER_INTEGRATION
--     URL = 's3://<your-bucket>/raw/customers/'
--     FILE_FORMAT = RAW.CUSTOMER_CSV_FORMAT
--     COMMENT = 'External stage over the raw customers S3 prefix';

-- ---------------------------------------------------------------------------
-- OPTION B (simplest for local/dev testing): CREDENTIALS-BASED STAGE
-- Stores an AWS access key/secret directly on the stage. Fine for a demo,
-- avoid for production (rotate/scope the key tightly if you must use it).
-- ---------------------------------------------------------------------------
CREATE STAGE IF NOT EXISTS RAW.CUSTOMER_S3_STAGE
    URL = 's3://<your-bucket>/raw/customers/'
    CREDENTIALS = (AWS_KEY_ID = '<aws_access_key_id>' AWS_SECRET_KEY = '<aws_secret_access_key>')
    FILE_FORMAT = RAW.CUSTOMER_CSV_FORMAT
    COMMENT = 'External stage over the raw customers S3 prefix (dev/demo credentials auth)';
