-- =============================================================================
-- stg_customers.sql
-- Staging layer: 1:1 with the raw source, but cleaned and typed.
--   * Casts id -> NUMBER, dob -> DATE
--   * Trims/normalizes whitespace and casing on text fields
--   * Standardizes country names to ISO-2 codes via the country_code_mapping
--     seed (falls back to the raw, trimmed value if no mapping exists)
--   * De-duplicates on customer_id, keeping the most recently loaded row
-- Materialized as a view: cheap to keep fresh, no storage cost, since marts
-- do the heavy lifting downstream.
-- =============================================================================

with source as (

    select *
    from {{ source('raw', 'customers_raw') }}

),

cleaned as (

    select
        try_cast(trim(customer_id) as number)      as customer_id,
        nullif(trim(name), '')                      as full_name,
        upper(trim(country))                        as country_name_raw,
        try_cast(trim(dob) as date)                  as date_of_birth,
        _loaded_at,
        _file_name

    from source

),

-- Standardize free-text country names to ISO-2 codes where we have a
-- mapping; otherwise keep the cleaned raw value so we never silently drop
-- data due to an incomplete mapping table.
standardized as (

    select
        cleaned.customer_id,
        cleaned.full_name,
        coalesce(country_map.country_code, cleaned.country_name_raw) as country_code,
        coalesce(country_map.country_name, cleaned.country_name_raw) as country_name,
        cleaned.date_of_birth,
        cleaned._loaded_at,
        cleaned._file_name

    from cleaned
    left join {{ ref('country_code_mapping') }} as country_map
        on upper(trim(country_map.country_name)) = cleaned.country_name_raw

),

-- Guard against duplicate customer_ids across historical loads (e.g. the
-- source system resending a full extract) by keeping only the latest row
-- per customer_id.
deduplicated as (

    select
        *,
        row_number() over (
            partition by customer_id
            order by _loaded_at desc
        ) as _row_num

    from standardized
    where customer_id is not null

)

select
    customer_id,
    full_name,
    country_code,
    country_name,
    date_of_birth,
    _loaded_at,
    _file_name
from deduplicated
where _row_num = 1
