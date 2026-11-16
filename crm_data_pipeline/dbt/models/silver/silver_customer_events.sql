{{ config(materialized='incremental', unique_key='event_id', incremental_strategy='merge', on_schema_change='append_new_columns') }}
select event_id, source_pk as customer_id, operation, source_commit_ts, source_lsn,
  payload:email::string as email, payload:first_name::string as first_name,
  payload:last_name::string as last_name, payload:phone::string as phone,
  payload:created_at::timestamp_tz as created_at,
  coalesce(payload:updated_at::timestamp_tz,source_commit_ts) as updated_at,
  ingested_at, source_file, raw_record
from {{ ref('stg_crm_events') }}
where entity='customers' and validation_error is null
{% if is_incremental() %}
and ingested_at >= dateadd(hour,-{{ var('crm_lookback_hours',72) }},
  (select coalesce(max(ingested_at),'1900-01-01'::timestamp_ntz) from {{ this }}))
{% endif %}
qualify row_number() over (partition by event_id order by ingested_at desc,source_commit_ts desc)=1
