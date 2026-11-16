{{ config(materialized='table') }}
select o.order_id,o.customer_id,o.product_id,o.quantity,o.unit_price,
 (o.quantity*o.unit_price)::number(18,2) as gross_line_amount,o.currency,o.status,
 o.created_at as order_created_at,o.updated_at as order_updated_at,
 c.email as customer_email,p.sku as product_sku,p.product_name,
 current_timestamp()::timestamp_ntz as dw_updated_at
from {{ ref('silver_orders_current') }} o
left join {{ ref('dim_customers') }} c on o.customer_id=c.customer_id
left join {{ ref('dim_products') }} p on o.product_id=p.product_id
