{{ config(materialized='view') }}
with extracted as (
  select RAW_RECORD as raw_record,
    RAW_RECORD:entity::string as entity,
    upper(coalesce(RAW_RECORD:operation::string,'UPSERT')) as operation,
    coalesce(RAW_RECORD:source_pk::string, RAW_RECORD:payload:customer_id::string,
      RAW_RECORD:payload:order_id::string, RAW_RECORD:payload:product_id::string) as source_pk,
    RAW_RECORD:event_id::string as event_id,
    RAW_RECORD:source_commit_ts::timestamp_tz as source_commit_ts,
    RAW_RECORD:source_lsn::string as source_lsn,
    RAW_RECORD:payload as payload,
    SOURCE_FILE as source_file, SOURCE_ROW_NUMBER as source_row_number,
    INGESTED_AT as ingested_at, LOAD_BATCH_ID as load_batch_id
  from {{ source('bronze','cdc_events') }}
)
select *,
  case
    when entity not in ('customers','orders','products') then 'UNKNOWN_ENTITY'
    when operation not in ('UPSERT','INSERT','UPDATE','DELETE') then 'UNKNOWN_OPERATION'
    when source_pk is null then 'MISSING_SOURCE_PK'
    when event_id is null then 'MISSING_EVENT_ID'
    when source_commit_ts is null then 'MISSING_SOURCE_COMMIT_TS'
    when payload is null and operation <> 'DELETE' then 'MISSING_PAYLOAD'
    else null
  end as validation_error
from extracted
