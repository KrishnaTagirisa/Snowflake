{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='order_id',
        cluster_by=['order_date'],
        on_schema_change='append_new_columns'
    )
}}

-- Grain: one row per order line.
-- Customer is resolved to the SCD2 version that was valid when the order was placed.
-- Unmatched keys fall back to the '-1' unknown member, so facts are never dropped.

with lines as (

    select *
    from {{ ref('stg_order_items') }}
    {% if is_incremental() %}
    where _dbt_updated_at > (select coalesce(max(_silver_updated_at), '1900-01-01'::timestamp_ltz) from {{ this }})
    {% endif %}

),

orders as (

    select order_id, customer_id, order_ts, order_status, sales_channel, currency, shipping_amount, item_count
    from {{ ref('stg_orders') }}

)

select
    l.order_line_id,
    l.order_id,
    l.line_number,
    to_number(to_char(l.order_date, 'YYYYMMDD'))            as order_date_key,
    l.order_date,
    o.order_ts,
    coalesce(c.customer_sk, '-1')                           as customer_sk,
    coalesce(p.product_sk, '-1')                            as product_sk,
    o.customer_id,
    l.product_id,
    o.order_status,
    o.sales_channel,
    o.currency,
    l.quantity,
    l.unit_price,
    l.gross_amount,
    l.discount_amount,
    l.net_amount,
    -- allocate order-level shipping evenly across lines so it sums correctly
    round(o.shipping_amount / nullif(o.item_count, 0), 2)   as allocated_shipping_amount,
    o.order_status in ('CANCELLED', 'RETURNED')             as is_cancelled_or_returned,
    l._dbt_updated_at                                       as _silver_updated_at,
    current_timestamp()::timestamp_ltz                      as _gold_loaded_at
from lines l
inner join orders o
    on o.order_id = l.order_id
left join {{ ref('dim_customer') }} c
    on  c.customer_id = o.customer_id
    and o.order_ts >= c.valid_from
    and o.order_ts <  c.valid_to
left join {{ ref('dim_product') }} p
    on p.product_id = l.product_id
