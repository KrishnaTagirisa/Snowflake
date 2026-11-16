# CRM Medallion Pipeline

Reference starter for a CRM with customers, orders and products using AWS DMS -> S3 -> Snowpipe -> Snowflake Bronze -> dbt Silver/Gold, orchestrated by Airflow, with Python and SQL tests.

## Important
This is a runnable transformation/orchestration starter, not a turnkey deployment for an unspecified CRM. Adapt DMS payload normalization, Snowflake integrations, IAM trust policies, credentials, alert routing and source-specific CDC semantics before production. Test in non-production first.

## Flow
1. CRM database -> AWS DMS full load + CDC -> encrypted S3 landing.
2. S3 notifications -> Snowpipe auto-ingest -> `CRM_ANALYTICS.BRONZE.CDC_EVENTS`.
3. Airflow runs dbt source freshness and `dbt build` every five minutes.
4. dbt parses events, creates Silver event/current-state models, then Gold dimensions and order fact.
5. pytest validates Python contract utilities; dbt tests validate keys, relationships, and business rules.

For seconds-level row ingestion, consider Snowpipe Streaming and implement durable source checkpoints. File-based Snowpipe is usually a minutes-oriented path and has no hard latency guarantee.

## Event contract
```json
{
  "entity": "customers",
  "operation": "UPSERT",
  "source_pk": "C-1001",
  "source_commit_ts": "2026-11-16T12:00:00Z",
  "source_lsn": "00000042",
  "event_id": "crm.customers.00000042.C-1001",
  "payload": {"customer_id":"C-1001","email":"a@example.com","updated_at":"2026-11-16T12:00:00Z"}
}
```
Normalize source-specific DMS output to this contract. Delete events should include `operation: DELETE`, primary key, timestamp, LSN and event ID; payload may be absent.

## Setup
- Provision S3 using `aws/` Terraform, then configure DMS replication instance, source endpoint, S3 target endpoint, table mappings and CDC task separately.
- Configure Snowflake storage and notification integrations using approved cross-account IAM.
- Review `snowflake/bootstrap.sql`, replace placeholders, and execute with a provisioning role.
- Copy `dbt/profiles.yml.example` to your dbt profile directory and set environment variables.
- Install `requirements.txt`; run `dbt deps`, `dbt debug`, `dbt build` from `dbt/`.
- Deploy `airflow/dags/crm_medallion_dag.py` with dbt available on workers.
- Run `pytest -q` from repository root.

## Environment
`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`. Prefer key-pair authentication or workload identity and secret-backed Airflow connections in production. Set `CRM_DBT_PROJECT_DIR` and `CRM_DBT_PROFILES_DIR`.

## Reliability and data handling
- At-least-once ingestion + idempotent merges; do not claim end-to-end exactly-once.
- Event ID deduplication; deterministic current-state selection by source commit timestamp, then source LSN. Normalize LSN if lexical ordering is unsafe.
- Deletes are tombstones and excluded from current-state entities.
- Late arrivals are covered by a configurable 72-hour ingestion lookback; older events require controlled replay/backfill.
- Raw VARIANT preserves unknown fields. Invalid events are flagged and a dbt assertion fails the build; add a durable quarantine table and alerting before production.
- Order/customer relationships are tested. Decide whether orphan records fail or warn based on business rules.
- This starter's Gold entities are current-state models. Use dbt snapshots/history models for SCD Type 2 requirements.
- PII access controls, masking, retention, deletion propagation, audit, encryption, and legal retention rules must be configured for your organization.
- Cross-entity source transaction consistency is not guaranteed unless transaction IDs and completeness markers are propagated.

## Production checklist
- Pin versions and use matching Airflow constraints.
- Separate dev/test/prod roles, databases and warehouses.
- Enable S3 block-public-access, versioning, KMS, lifecycle, access logs and least-privilege policies.
- Configure DMS recovery/checkpoints, error logging, full-load + CDC, and schema/table mappings.
- Add alert destinations, source-target count/amount reconciliation, latency SLOs, run audit, and failed-file quarantine.
- Add CI (`dbt parse`, `dbt build` against test data, pytest), code review, SQL linting and artifact retention.
- Add Snowflake resource monitors, query tags, auto-suspend, masking policies and cost alerts.
- Test retry, duplicate, out-of-order, late, delete, schema drift, invalid types, replay, full rebuild and disaster recovery.

## Official references
- Snowpipe: https://docs.snowflake.com/en/user-guide/data-load-snowpipe-intro
- Snowpipe Streaming: https://docs.snowflake.com/en/user-guide/snowpipe-streaming/data-load-snowpipe-streaming-overview
- dbt incremental models: https://docs.getdbt.com/docs/build/incremental-models
- dbt data tests: https://docs.getdbt.com/docs/build/data-tests
- Airflow best practices: https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html
