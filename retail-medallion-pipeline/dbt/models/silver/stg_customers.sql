{{
    config(
        unique_key='customer_id',
        cluster_by=['customer_id']
    )
}}

-- Grain: one row per customer (latest known version).
-- Bronze holds a full snapshot every day, so the same customer appears many times;
-- we keep the most recent version and never let an older late file overwrite a newer row.

with source as (

    select *
    from {{ source('bronze', 'customers_raw') }}
    {{ incremental_filter('_loaded_at', '_bronze_loaded_at') }}

),

typed as (

    select
        upper({{ clean_str('customer_id') }})                       as customer_id,
        initcap({{ clean_str('first_name') }})                      as first_name,
        initcap({{ clean_str('last_name') }})                       as last_name,
        lower({{ clean_str('email') }})                             as email,
        regexp_replace({{ clean_str('phone') }}, '[^0-9+]', '')     as phone,
        initcap({{ clean_str('city') }})                            as city,
        upper({{ clean_str('country') }})                           as country_code,
        coalesce(upper({{ clean_str('segment') }}), 'UNKNOWN')      as segment,
        {{ parse_ts('created_at') }}                                as created_at,
        {{ parse_ts('updated_at') }}                                as updated_at,
        _source_file,
        _loaded_at                                                  as _bronze_loaded_at
    from source
    where {{ clean_str('customer_id') }} is not null

),

deduplicated as (

    select *
    from typed
    qualify row_number() over (
        partition by customer_id
        order by updated_at desc nulls last, _bronze_loaded_at desc
    ) = 1

)

select
    d.customer_id,
    d.first_name,
    d.last_name,
    trim(concat_ws(' ', d.first_name, d.last_name))                as full_name,
    d.email,
    regexp_like(d.email, '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$')         as is_email_valid,
    d.phone,
    d.city,
    d.country_code,
    d.segment,
    d.created_at,
    d.updated_at,
    d._source_file,
    d._bronze_loaded_at,
    {{ silver_audit_columns() }}
from deduplicated d
{% if is_incremental() %}
left join {{ this }} t
    on t.customer_id = d.customer_id
-- protect against late-arriving older files overwriting newer data
where t.customer_id is null
   or d.updated_at >= t.updated_at
   or t.updated_at is null
{% endif %}
