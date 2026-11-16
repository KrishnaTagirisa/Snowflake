# Retail Medallion Pipeline

S3 → Snowflake **Bronze → Silver → Gold**, transformed with **dbt**, orchestrated by **Airflow** (Cosmos).

```
OMS extracts ──► S3 landing ──► BRONZE ──────► SILVER ───────────► GOLD ──────────► Power BI
(CSV / NDJSON)   load_date=     COPY INTO,     dbt incremental     dim_customer (SCD2)
                 partitions +   raw VARCHAR /  merge, dedup,       dim_product, dim_date
                 _SUCCESS       VARIANT +      typing, flatten,    fct_order_lines
                                audit cols     snapshot (SCD2)     agg_daily_sales
       └──────────────────────────── Airflow DAG: retail_medallion_daily ────────────────────┘
```

## Repository layout

| Path | What it is |
|---|---|
| `infra/aws/` | S3 bucket setup (KMS, versioning, lifecycle), IAM policies for Snowflake + Airflow |
| `infra/snowflake/01–05` | Roles, warehouses, resource monitor, DB/schemas, storage integration, stage, file formats, bronze + audit tables, COPY templates, optional Snowpipe |
| `infra/snowflake/06_validation_queries.sql` | Queries with expected results that prove each edge case works |
| `dbt/` | dbt project: sources, silver models, snapshot, gold star schema, tests, macros |
| `airflow/dags/retail_medallion_daily.py` | The DAG |
| `airflow/dags/retail/bronze_loader.py` | COPY INTO + audit logic (unit-tested) |
| `airflow/tests/` | Unit tests and DAG integrity tests |
| `sample_data/` | Generator for two days of test files covering every edge case |
| `.github/workflows/ci.yml` | Lint, tests, manifest build, dbt slim CI |
| `Dockerfile` | Airflow image with dbt in an isolated venv |

## Design decisions

**Bronze**
- Schema-on-read: CSV columns land as `VARCHAR`, JSON as `VARIANT`, so malformed values never fail ingestion.
- Every row carries `_source_file`, `_source_row`, `_file_last_modified`, `_loaded_at`, `_batch_id` for lineage.
- `COPY INTO` is scoped to one `load_date=` partition; Snowflake's load metadata skips files already loaded, so reruns are idempotent. A rerun that copies nothing passes only if the partition was loaded before; otherwise it fails as "no files found".
- Every file result (and any aborted COPY) is written to `AUDIT.LOAD_LOG`.

**Silver**
- Incremental `merge` on business key, filtered by `_loaded_at` minus `lookback_hours` (default 24) to catch late files.
- Deduplicate with `QUALIFY ROW_NUMBER()` by `updated_at`, and a join to the existing row stops an older late file overwriting newer data.
- `stg_order_items` uses `delete+insert` on `order_id`, so lines removed from an amended order disappear.
- `snap_customers` is a timestamp-strategy snapshot providing SCD2 history.

**Gold**
- `dim_customer` is SCD2: first version opens from 1900-01-01, the current version ends at 9999-12-31. Facts join on `order_ts` within `[valid_from, valid_to)` to get the customer version valid at order time.
- Unknown members (`'-1'`) mean unmatched facts are never dropped.
- `fct_order_lines` is incremental (`delete+insert` by order), clustered on `order_date`.
- `dim_date` includes the UK fiscal year (April start).

**Orchestration**
- `CronTriggerTimetable` at 06:30 Europe/London loads the previous day's extract; trigger with `{"load_date": "YYYY-MM-DD"}` for backfills, and `{"full_refresh": true}` to rebuild incremental models.
- Deferrable S3 sensor waits on the `_SUCCESS` marker without holding a worker slot.
- Bronze loads use dynamic task mapping (one task per entity, in parallel).
- Cosmos renders each dbt model as `run` + `test` tasks, and bronze sources as freshness/test tasks at the start of the silver group. A failing test stops only its downstream branch, and a retry reruns one model, not the whole project.
- `max_active_runs=1`, exponential retries, Slack alert on any task failure.

**Security**
- No access keys: Snowflake reads S3 through a storage integration (IAM role and external ID).
- Key-pair auth for service users; credentials in the Airflow connection, backed by AWS Secrets Manager.
- Least-privilege roles (loader, transformer, reporter), managed-access schemas, a warehouse per workload, and a resource monitor.

## Setup

1. **AWS:** run `infra/aws/s3_bucket_setup.sh`, then create IAM role `snowflake-retail-landing-reader` with `snowflake_s3_read_policy.json`.
2. **Snowflake:** run `01` → `03`. After `02`, copy `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID` from `DESC INTEGRATION` into `snowflake_role_trust_policy.json` and apply it to the IAM role.
3. **Airflow connections:**
   - `snowflake_default`: login `SVC_AIRFLOW`; password = private key passphrase; extras `{"account": "...", "database": "RETAIL_PROD", "warehouse": "LOAD_WH", "role": "RETAIL_LOADER", "private_key_content": "<PEM>"}`
   - `aws_default`: IAM role with `airflow_s3_sensor_policy.json`
   - `slack_data_alerts` (optional)
4. **Image:** `docker build -t retail-airflow .` (installs dbt in its own venv and pre-builds `manifest.json`).
5. **Test with sample data:**
   ```bash
   python sample_data/generate_sample_data.py
   aws s3 sync sample_data/out/retail s3://acme-retail-landing/retail/
   ```
   Trigger the DAG with `{"load_date": "2026-11-10"}`, then `{"load_date": "2026-11-11"}`, and run `infra/snowflake/06_validation_queries.sql`.

## Running locally

```bash
# dbt
cd dbt && export SNOWFLAKE_ACCOUNT=... SNOWFLAKE_USER=... SNOWFLAKE_PRIVATE_KEY_PATH=~/.ssh/sf.p8
dbt deps && dbt build --profiles-dir .                  # builds into RETAIL_DEV.DBT_<you>_SILVER/GOLD
dbt build --profiles-dir . --select stg_orders+         # one model and everything downstream
dbt build --profiles-dir . --full-refresh -s tag:silver # rebuild incrementals

# Airflow tests
pip install -r requirements-airflow.txt -r requirements-dev.txt
PYTHONPATH=airflow/dags DBT_PROJECT_DIR=$PWD/dbt pytest airflow/tests -q
```

## Operational notes

- **Late-arriving customer:** an order whose customer isn't loaded yet gets `customer_sk = '-1'`. A weekly `full_refresh` of `fct_order_lines` (or a targeted re-key job) repairs those rows.
- **Snowpipe instead of batch:** use `05_snowpipe_optional.sql`; the DAG then drops the sensor and load tasks and keeps the dbt groups.
- **Load metadata expiry:** Snowflake tracks loaded files for 64 days. For older backfills, delete the partition's bronze rows first, then reload.
- **Downstream triggers:** add an Airflow Asset as an outlet of `publish_run_summary` to trigger a Power BI refresh DAG.
