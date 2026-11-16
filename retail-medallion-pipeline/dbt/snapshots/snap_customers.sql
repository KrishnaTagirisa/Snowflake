{% snapshot snap_customers %}

{{
    config(
        schema='silver',
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='updated_at',
        dbt_valid_to_current="to_timestamp_ntz('9999-12-31 00:00:00')"
    )
}}

-- SCD Type 2 history of customers. A new version is opened whenever updated_at moves.
select
    customer_id,
    first_name,
    last_name,
    full_name,
    email,
    phone,
    city,
    country_code,
    segment,
    created_at,
    updated_at
from {{ ref('stg_customers') }}

{% endsnapshot %}
