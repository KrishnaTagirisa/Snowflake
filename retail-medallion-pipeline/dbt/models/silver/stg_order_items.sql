{{
    config(
        incremental_strategy='delete+insert',
        unique_key='order_id'
    )
}}

-- Grain: one row per order line.
-- delete+insert on order_id: when an order is re-sent, ALL its old lines are removed
-- and the new set inserted, so lines dropped from an amended order don't linger.

with orders as (

    select order_id, order_date, items_raw, _dbt_updated_at as _order_updated_at
    from {{ ref('stg_orders') }}
    {% if is_incremental() %}
    where _dbt_updated_at > (select coalesce(max(_order_updated_at), '1900-01-01'::timestamp_ltz) from {{ this }})
    {% endif %}

),

flattened as (

    select
        o.order_id,
        o.order_date,
        coalesce(i.value:line_number::int, i.index + 1)             as line_number,
        upper(i.value:product_id::varchar)                         as product_id,
        try_to_number(i.value:quantity::varchar)                   as quantity,
        try_to_decimal(i.value:unit_price::varchar, 12, 2)         as unit_price,
        coalesce(try_to_decimal(i.value:discount_amount::varchar, 12, 2), 0) as discount_amount,
        o._order_updated_at
    from orders o,
         lateral flatten(input => o.items_raw, outer => false) i

)

select
    order_id || '-' || lpad(line_number::varchar, 3, '0')   as order_line_id,
    order_id,
    order_date,
    line_number,
    product_id,
    quantity,
    unit_price,
    discount_amount,
    round(quantity * unit_price, 2)                       as gross_amount,
    round(quantity * unit_price - discount_amount, 2)     as net_amount,
    _order_updated_at,
    {{ silver_audit_columns() }}
from flattened
