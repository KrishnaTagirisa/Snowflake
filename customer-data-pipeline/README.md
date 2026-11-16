# customer-data-pipeline

Production-style data pipeline that lands a customer CSV from **S3** into
**Snowflake**, orchestrated by **Apache Airflow**, transformed with **dbt**.

Sample/synthetic data (`id, name, country, dob`) is included so the whole
project can be explored end-to-end without a real source system. See
`docs/architecture.md` for the full design write-up and data flow diagram.

## Stack

| Layer | Tool |
|---|---|
| Storage (source) | AWS S3 |
| Warehouse | Snowflake |
| Orchestration | Apache Airflow 2.9 |
| Transformation | dbt (dbt-snowflake) |
| Local runtime | Docker Compose |
| CI | GitHub Actions |

## Project structure

```
customer-data-pipeline/
├── dags/
│   └── customer_s3_to_snowflake_pipeline.py   # the Airflow DAG
├── data/
│   └── sample_customers.csv                   # synthetic sample data (id, name, country, dob)
├── scripts/
│   └── generate_sample_data.py                # regenerate / upload sample data to S3
├── snowflake/ddl/                              # one-time Snowflake object setup, run in order
│   ├── 01_setup_database.sql
│   ├── 02_create_file_format.sql
│   ├── 03_create_stage.sql
│   ├── 04_create_raw_table.sql
│   └── 05_create_audit_table.sql
├── dbt_project/
│   ├── models/staging/    # cleaned/typed/de-duplicated (stg_customers)
│   ├── models/marts/      # BI-ready dimension (dim_customers)
│   ├── seeds/             # country_code_mapping.csv
│   ├── snapshots/         # SCD2 history (customers_snapshot)
│   ├── macros/            # generate_schema_name.sql
│   └── tests/             # singular test: assert_no_future_or_implausible_dob
├── tests/
│   └── test_dag_integrity.py                  # pytest: DAG imports cleanly, no cycles
├── .github/workflows/ci.yml                    # lint + dbt parse + DAG tests on every PR
├── docker-compose.yml + Dockerfile              # local Airflow stack
├── requirements.txt
└── .env.example
```

## Prerequisites

- Docker + Docker Compose (for local Airflow)
- A Snowflake account (trial account works fine)
- An AWS account + S3 bucket (or use LocalStack/MinIO for a fully local demo)
- Python 3.11 (only needed if running scripts/dbt outside Docker)

## Setup

### 1. Provision Snowflake objects

Run the scripts in `snowflake/ddl/` **in order** as a role with sufficient
privileges (e.g. `SYSADMIN`, then `ACCOUNTADMIN` for the storage
integration if you use Option A in `03_create_stage.sql`):

```sql
-- in Snowsql or the Snowflake web UI
!source snowflake/ddl/01_setup_database.sql
!source snowflake/ddl/02_create_file_format.sql
!source snowflake/ddl/03_create_stage.sql   -- fill in your bucket/credentials first
!source snowflake/ddl/04_create_raw_table.sql
!source snowflake/ddl/05_create_audit_table.sql
```

### 2. Upload the sample data to S3

```bash
pip install -r requirements.txt
python scripts/generate_sample_data.py --rows 200 --upload \
    --bucket <your-bucket> --key raw/customers/customers.csv
```

(A pre-generated `data/sample_customers.csv` is already included if you'd
rather upload it manually via the AWS console/CLI.)

### 3. Configure environment variables

```bash
cp .env.example .env
# edit .env with your AWS + Snowflake credentials
```

### 4. Start Airflow locally

```bash
docker compose up airflow-init   # one-time DB init + admin user
docker compose up -d
```

Airflow UI: http://localhost:8080 (default admin/admin, from `airflow-init`).

### 5. Configure Airflow Connections

In the Airflow UI (Admin → Connections), create:

- `aws_default` — AWS access key/secret (or an IAM role if running on ECS/EC2)
- `snowflake_default` — account, user, password, warehouse, database, role

And optionally override the Airflow Variables used by the DAG (Admin →
Variables): `customer_pipeline_s3_bucket`, `customer_pipeline_s3_key`, etc.
(sane defaults are hardcoded so the DAG runs without any Variables set, but
production deployments should always set these explicitly).

### 6. Trigger the pipeline

Unpause and trigger `customer_s3_to_snowflake_pipeline` from the Airflow UI,
or:

```bash
docker compose exec airflow-scheduler airflow dags trigger customer_s3_to_snowflake_pipeline
```

### 7. Run dbt standalone (optional, outside Airflow)

```bash
cd dbt_project
cp profiles.yml.example profiles.yml
export $(cat ../.env | xargs)   # load env vars referenced by profiles.yml
dbt deps
dbt seed
dbt run
dbt test
```

## What the DAG does

1. `wait_for_source_file` — S3 sensor, waits for `customers.csv`.
2. `validate_source_file` — pandas schema/null checks before touching Snowflake.
3. `load_raw_to_snowflake` — `COPY INTO RAW.CUSTOMERS_RAW` from the external stage.
4. `run_data_quality_checks` — row count + null-key checks in Snowflake.
5. `log_pipeline_audit` — writes a row to `OPS.PIPELINE_LOAD_AUDIT`.
6. `dbt_deps` / `dbt_run` / `dbt_test` — transforms RAW → staging → marts and runs all dbt tests.
7. `notify_success` — stub for Slack/email/PagerDuty alerting.

Full design rationale (idempotency, data quality layers, security) is in
`docs/architecture.md`.

## Testing

```bash
pytest tests/            # DAG integrity (imports cleanly, no cycles, expected tasks present)
cd dbt_project && dbt test   # data quality tests against a live Snowflake connection
```

CI (`.github/workflows/ci.yml`) runs the pytest suite and `dbt parse` (schema/Jinja
validation without needing live Snowflake credentials) on every PR.

## Assumptions made for this demo

- Sample data is synthetic (`scripts/generate_sample_data.py`, seeded for
  reproducibility) since no real source system/S3 file was provided.
- Snowflake authentication uses a password for simplicity; production should
  use key-pair auth (noted inline in `snowflake/ddl/01_setup_database.sql`).
- The S3 stage uses credentials-based auth by default; the storage
  integration (IAM role trust) approach is included but commented out, since
  it requires account-specific IAM setup.
- Notifications are stubbed to a log line — wire up Slack/email/PagerDuty in
  `dags/customer_s3_to_snowflake_pipeline.py::alert_on_failure`.
