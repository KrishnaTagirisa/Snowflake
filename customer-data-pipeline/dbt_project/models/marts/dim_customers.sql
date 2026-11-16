-- =============================================================================
-- dim_customers.sql
-- Marts layer: the customer dimension consumed by BI tools / analysts.
--   * Surrogate key via dbt_utils (stable, hash-based) decouples downstream
--     models from the source system's raw ID scheme.
--   * Adds derived attributes (age, age_band, is_adult) so consumers don't
--     recompute the same logic in every dashboard.
-- Materialized as a table (see dbt_project.yml) for fast BI query performance.
-- =============================================================================

with stg_customers as (

    select * from {{ ref('stg_customers') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_sk,
        customer_id,
        full_name,
        country_code,
        country_name,
        date_of_birth,
        datediff(year, date_of_birth, current_date()) as age,
        case
            when datediff(year, date_of_birth, current_date()) < 18 then 'Under 18'
            when datediff(year, date_of_birth, current_date()) between 18 and 34 then '18-34'
            when datediff(year, date_of_birth, current_date()) between 35 and 54 then '35-54'
            else '55+'
        end as age_band,
        datediff(year, date_of_birth, current_date()) >= 18 as is_adult,
        _loaded_at as source_loaded_at,
        current_timestamp() as dbt_updated_at

    from stg_customers

)

select * from final
