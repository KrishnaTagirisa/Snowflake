{{
    config(
        unique_key='product_id'
    )
}}

-- Grain: one row per product (latest known version, SCD1).

with source as (

    select *
    from {{ source('bronze', 'products_raw') }}
    {{ incremental_filter('_loaded_at', '_bronze_loaded_at') }}

),

typed as (

    select
        upper({{ clean_str('product_id') }})                         as product_id,
        {{ clean_str('product_name') }}                              as product_name,
        coalesce(initcap({{ clean_str('category') }}), 'Unknown')    as category,
        coalesce(initcap({{ clean_str('sub_category') }}), 'Unknown') as sub_category,
        {{ clean_str('brand') }}                                     as brand,
        try_to_decimal(replace(unit_price, ',', ''), 12, 2)         as unit_price,
        coalesce(upper({{ clean_str('currency') }}), 'GBP')          as currency,
        coalesce(try_to_boolean(is_active), true)                    as is_active,
        {{ parse_ts('updated_at') }}                                 as updated_at,
        _source_file,
        _loaded_at                                                   as _bronze_loaded_at
    from source
    where {{ clean_str('product_id') }} is not null

),

deduplicated as (

    select *
    from typed
    qualify row_number() over (
        partition by product_id
        order by updated_at desc nulls last, _bronze_loaded_at desc
    ) = 1

)

select
    d.*,
    {{ silver_audit_columns() }}
from deduplicated d
{% if is_incremental() %}
left join {{ this }} t
    on t.product_id = d.product_id
where t.product_id is null
   or d.updated_at >= t.updated_at
   or t.updated_at is null
{% endif %}
