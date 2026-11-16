-- =============================================================================
-- 02_storage_integration_and_stage.sql
-- Secure S3 access (no keys), file formats and external stage.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Storage integration (requires ACCOUNTADMIN or CREATE INTEGRATION privilege)
-- -----------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

CREATE STORAGE INTEGRATION IF NOT EXISTS S3_RETAIL_LANDING_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<AWS_ACCOUNT_ID>:role/snowflake-retail-landing-reader'
  STORAGE_ALLOWED_LOCATIONS = ('s3://acme-retail-landing/retail/');

-- Copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID from this output
-- into infra/aws/snowflake_role_trust_policy.json, then update the IAM role.
DESC INTEGRATION S3_RETAIL_LANDING_INT;

GRANT USAGE ON INTEGRATION S3_RETAIL_LANDING_INT TO ROLE RETAIL_LOADER;

-- -----------------------------------------------------------------------------
-- 2. File formats
-- -----------------------------------------------------------------------------
USE ROLE RETAIL_LOADER;
USE DATABASE RETAIL_PROD;
USE SCHEMA BRONZE;

CREATE FILE FORMAT IF NOT EXISTS FF_CSV
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  TRIM_SPACE = TRUE
  EMPTY_FIELD_AS_NULL = TRUE
  NULL_IF = ('', 'NULL', 'null')
  ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
  ENCODING = 'UTF8'
  COMPRESSION = AUTO;

CREATE FILE FORMAT IF NOT EXISTS FF_JSON
  TYPE = JSON
  STRIP_OUTER_ARRAY = TRUE   -- handles both NDJSON and [ {...}, {...} ]
  STRIP_NULL_VALUES = FALSE
  COMPRESSION = AUTO;

-- -----------------------------------------------------------------------------
-- 3. External stage over the landing bucket
-- -----------------------------------------------------------------------------
CREATE STAGE IF NOT EXISTS STG_RETAIL_LANDING
  URL = 's3://acme-retail-landing/retail/'
  STORAGE_INTEGRATION = S3_RETAIL_LANDING_INT
  DIRECTORY = (ENABLE = TRUE)
  COMMENT = 'Daily OMS extracts: <entity>/load_date=YYYY-MM-DD/<file>';

-- Smoke test
LIST @STG_RETAIL_LANDING/customers/;
