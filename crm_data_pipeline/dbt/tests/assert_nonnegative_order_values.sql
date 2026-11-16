select * from {{ ref('silver_orders_current') }} where quantity < 0 or unit_price < 0
