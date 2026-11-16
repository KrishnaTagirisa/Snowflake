# Architecture

## Overview

This pipeline follows a standard **EL + T** (Extract-Load, then Transform)
pattern, layered as a medallion architecture inside a single Snowflake
database:

```
                 ┌──────────────┐
   Source        │   AWS S3     │   customers.csv lands daily at
   System   ───▶ │  (raw zone)  │   s3://<bucket>/raw/customers/
                 └──────┬───────┘
                        │  S3KeySensor (wait for file)
                        ▼
                 ┌──────────────┐
   Airflow       │  Validate    │   pandas: schema + null-id checks
   (orchestrate) │  source file │
                 └──────┬───────┘
                        │  COPY INTO (SnowflakeOperator)
                        ▼
                 ┌──────────────────────┐
   Snowflake     │ RAW.CUSTOMERS_RAW    │  minimally-typed landing table
   (RAW schema)  │ (medallion: bronze)  │  + load metadata (_loaded_at, etc.)
                 └──────┬───────────────┘
                        │  Airflow: post-load DQ checks + audit log
                        │  dbt run / dbt test
                        ▼
                 ┌──────────────────────┐
   dbt           │ stg_customers        │  cleaned, typed, de-duplicated
   (STAGING,     │ (medallion: silver)  │  (view)
   view)         └──────┬───────────────┘
                        ▼
                 ┌──────────────────────┐
   dbt           │ dim_customers        │  surrogate key, derived age/band
   (MARTS,       │ (medallion: gold)    │  (table, BI-ready)
   table)        └──────────────────────┘
                        ▲
                        │  dbt snapshot (SCD2)
                 ┌──────────────────────┐
                 │ customers_snapshot   │  historical change tracking
                 └──────────────────────┘
```

## Component responsibilities

**Airflow** owns orchestration and operational concerns only: detecting
new files, sequencing steps, retrying transient failures, and alerting.
It deliberately does *not* contain business transformation logic.

**Snowflake RAW schema** is a landing zone. Data is loaded close to its
source shape (light casting to text) so a load never fails due to a
transformation bug -- transformation failures should surface in dbt, where
they're easier to debug and fix without re-extracting from source.

**dbt** owns all transformation logic: typing, cleaning, de-duplication,
standardization (e.g. country codes), surrogate keys, derived attributes,
and data quality tests. This keeps transformation logic version-controlled,
testable, and independent of the orchestrator.

## Idempotency & reruns

- `COPY INTO` is idempotent per source file: Snowflake tracks loaded files
  and skips a file already loaded into the same table (`FORCE = FALSE`).
  Re-running the DAG for a given day is safe.
- `stg_customers` de-duplicates on `customer_id`, keeping the most recently
  loaded row, so re-loading a corrected file is also safe.
- dbt models are declarative (`CREATE OR REPLACE` under the hood for
  tables/views), so `dbt run` is safe to re-run at any point.

## Data quality strategy (defense in depth)

1. **Pre-load (Airflow/pandas):** required columns present, file not
   empty, no null IDs -- fails fast, before spending Snowflake compute.
2. **Post-load (Airflow/SQL):** row count sanity check, null-key check on
   the raw table -- catches COPY INTO-specific issues.
3. **Transform-time (dbt schema tests):** `unique`/`not_null` on keys in
   `stg_customers` and `dim_customers`.
4. **Transform-time (dbt singular test):** `assert_no_future_or_implausible_dob`
   catches semantically-invalid dates that generic tests can't express.
5. **Source freshness (dbt):** `sources.yml` freshness thresholds flag a
   stale/late source file.

## Security notes

- Prefer a Snowflake **storage integration** (IAM role trust) over static
  AWS keys on the external stage -- see the commented Option A in
  `snowflake/ddl/03_create_stage.sql`.
- Prefer **key-pair authentication** for the `svc_airflow` Snowflake user
  over a password in production.
- All credentials are sourced from environment variables / Airflow
  Connections, never hardcoded in DAG or dbt code.
- In production, back Airflow Variables/Connections with a secrets manager
  (AWS Secrets Manager, SSM Parameter Store, or HashiCorp Vault) via
  Airflow's secrets backend interface, rather than the metadata DB.
