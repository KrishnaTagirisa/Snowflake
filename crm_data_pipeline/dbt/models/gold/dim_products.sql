{{ config(materialized='table') }}
select product_id,sku,product_name,category,unit_price,currency,is_active,created_at,updated_at,
 current_timestamp()::timestamp_ntz as dw_updated_at
from {{ ref('silver_products_current') }}
