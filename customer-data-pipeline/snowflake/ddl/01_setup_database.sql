-- =============================================================================
-- 01_setup_database.sql
-- Run once by a role with sufficient privileges (e.g. SYSADMIN/SECURITYADMIN).
-- Creates the database, warehouse, and a dedicated service role/user that
-- Airflow uses to load data and that dbt uses to transform it.
-- =============================================================================

-- Warehouse sized small; auto-suspend keeps compute cost down between runs.
CREATE WAREHOUSE IF NOT EXISTS TRANSFORM_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse used by Airflow loads and dbt transformations';

-- Single database for the whole pipeline; RAW/STAGING/ANALYTICS are schemas
-- within it (medallion-style layering), not separate databases, to keep
-- cross-schema references simple for dbt.
CREATE DATABASE IF NOT EXISTS ANALYTICS
    COMMENT = 'Customer analytics pipeline database (raw -> staging -> marts)';

-- Dedicated role for the pipeline's service account. Following least-
-- privilege, this role only gets what it needs on this database/warehouse.
CREATE ROLE IF NOT EXISTS TRANSFORM_ROLE
    COMMENT = 'Role used by the Airflow/dbt service account for this pipeline';

GRANT USAGE ON WAREHOUSE TRANSFORM_WH TO ROLE TRANSFORM_ROLE;
GRANT ALL ON DATABASE ANALYTICS TO ROLE TRANSFORM_ROLE;

-- Service user used by Airflow's Snowflake connection and dbt's profile.
-- In production, prefer key-pair authentication over a password (see
-- README "Security" section) -- password shown here only for local/dev use.
CREATE USER IF NOT EXISTS svc_airflow
    PASSWORD = 'change_me_use_key_pair_auth_in_prod'
    DEFAULT_ROLE = TRANSFORM_ROLE
    DEFAULT_WAREHOUSE = TRANSFORM_WH
    MUST_CHANGE_PASSWORD = TRUE
    COMMENT = 'Service account used by Airflow + dbt for the customer pipeline';

GRANT ROLE TRANSFORM_ROLE TO USER svc_airflow;
