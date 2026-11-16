{{ config(materialized='table') }}
select customer_id,email,first_name,last_name,phone,created_at,updated_at,
 current_timestamp()::timestamp_ntz as dw_updated_at
from {{ ref('silver_customers_current') }}
