{{
    config(
        unique_key='order_id',
        cluster_by=['to_date(order_ts)']
    )
}}

-- Grain: one row per order (latest status).
-- The same order is re-sent when its status changes (PLACED -> SHIPPED -> DELIVERED),
-- so we keep the most recent version. The raw items[] array is carried through so
-- stg_order_items flattens exactly the same version of the order.

with source as (

    select *
    from {{ source('bronze', 'orders_raw') }}
    {{ incremental_filter('_loaded_at', '_bronze_loaded_at') }}

),

parsed as (

    select
        upper(payload:order_id::varchar)                         as order_id,
        upper(payload:customer_id::varchar)                      as customer_id,
        {{ parse_ts('payload:order_ts::varchar') }}              as order_ts,
        upper(coalesce(payload:status::varchar, 'UNKNOWN'))      as order_status,
        upper(coalesce(payload:channel::varchar, 'UNKNOWN'))     as sales_channel,
        coalesce(upper(payload:currency::varchar), 'GBP')        as currency,
        try_to_decimal(payload:shipping_amount::varchar, 12, 2)  as shipping_amount,
        try_to_decimal(payload:order_total::varchar, 12, 2)      as order_total_reported,
        {{ parse_ts('payload:updated_at::varchar') }}            as updated_at,
        payload:items                                            as items_raw,
        _source_file,
        _loaded_at                                               as _bronze_loaded_at
    from source
    where payload:order_id is not null

),

deduplicated as (

    select *
    from parsed
    qualify row_number() over (
        partition by order_id
        order by updated_at desc nulls last, _bronze_loaded_at desc
    ) = 1

)

select
    d.order_id,
    d.customer_id,
    d.order_ts,
    to_date(d.order_ts)                         as order_date,
    d.order_status,
    d.sales_channel,
    d.currency,
    coalesce(d.shipping_amount, 0)              as shipping_amount,
    d.order_total_reported,
    array_size(d.items_raw)                     as item_count,
    d.items_raw,
    d.updated_at,
    d._source_file,
    d._bronze_loaded_at,
    {{ silver_audit_columns() }}
from deduplicated d
{% if is_incremental() %}
left join {{ this }} t
    on t.order_id = d.order_id
where t.order_id is null
   or d.updated_at >= t.updated_at
   or t.updated_at is null
{% endif %}
