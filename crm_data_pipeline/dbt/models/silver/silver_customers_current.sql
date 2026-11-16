{{ config(materialized='table') }}
select * exclude (rn) from (
 select e.*, row_number() over (partition by customer_id
 order by source_commit_ts desc,source_lsn desc,ingested_at desc) rn
 from {{ ref('silver_customer_events') }} e
) where rn=1 and operation <> 'DELETE'
