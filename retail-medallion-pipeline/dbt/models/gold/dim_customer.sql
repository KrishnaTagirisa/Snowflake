-- SCD Type 2 customer dimension built from the snapshot.
-- The first version of each customer is opened from 1900-01-01 so orders placed
-- before our first observed change still find a matching version.

with history as (

    select
        customer_id,
        full_name,
        first_name,
        last_name,
        email,
        city,
        country_code,
        segment,
        created_at,
        dbt_scd_id,
        dbt_valid_from,
        dbt_valid_to,
        row_number() over (partition by customer_id order by dbt_valid_from) as version_number
    from {{ ref('snap_customers') }}

),

versions as (

    select
        {{ dbt_utils.generate_surrogate_key(['dbt_scd_id']) }}       as customer_sk,
        customer_id,
        full_name,
        first_name,
        last_name,
        email,
        city,
        country_code,
        segment,
        created_at,
        version_number,
        case when version_number = 1
             then '1900-01-01'::timestamp_ntz
             else dbt_valid_from end                                  as valid_from,
        dbt_valid_to                                                  as valid_to,
        dbt_valid_to = '9999-12-31'::timestamp_ntz                    as is_current
    from history

),

unknown_member as (

    select
        '-1'                          as customer_sk,
        '-1'                          as customer_id,
        'Unknown customer'            as full_name,
        null                          as first_name,
        null                          as last_name,
        null                          as email,
        null                          as city,
        null                          as country_code,
        'UNKNOWN'                     as segment,
        null::timestamp_ntz           as created_at,
        1                             as version_number,
        '1900-01-01'::timestamp_ntz   as valid_from,
        '9999-12-31'::timestamp_ntz   as valid_to,
        true                          as is_current

)

select * from versions
union all
select * from unknown_member
