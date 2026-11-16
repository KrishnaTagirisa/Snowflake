-- Fails if the order total reported by the source doesn't equal the sum of its
-- line net amounts + shipping (tolerance 1p for rounding).
{{ config(tags=['silver'], severity='warn', warn_if='>0', error_if='>50') }}

select
    o.order_id,
    o.order_total_reported,
    sum(l.net_amount) + o.shipping_amount           as order_total_calculated,
    abs(o.order_total_reported - (sum(l.net_amount) + o.shipping_amount)) as difference
from {{ ref('stg_orders') }} o
inner join {{ ref('stg_order_items') }} l
    on l.order_id = o.order_id
where o.order_total_reported is not null
group by o.order_id, o.order_total_reported, o.shipping_amount
having abs(o.order_total_reported - (sum(l.net_amount) + o.shipping_amount)) > 0.01
