-- SCD Type 1 product dimension (current attributes only).
-- Price at the time of sale is captured on the fact, so history isn't needed here.

select
    {{ dbt_utils.generate_surrogate_key(['product_id']) }}   as product_sk,
    product_id,
    product_name,
    category,
    sub_category,
    brand,
    unit_price                                            as current_list_price,
    currency,
    is_active,
    updated_at                                            as source_updated_at
from {{ ref('stg_products') }}

union all

select
    '-1', '-1', 'Unknown product', 'Unknown', 'Unknown', null,
    null, null, false, null
