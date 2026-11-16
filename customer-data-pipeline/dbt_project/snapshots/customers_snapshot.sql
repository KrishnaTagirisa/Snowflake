-- =============================================================================
-- customers_snapshot.sql
-- Tracks slowly-changing attributes (name, country, dob) on the raw source
-- over time using dbt's SCD Type 2 snapshot pattern. Lets analysts answer
-- "what did we know about this customer as of date X" -- useful for audit
-- and for correcting downstream aggregates if a source record is corrected.
-- =============================================================================

{% snapshot customers_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='check',
        check_cols=['name', 'country', 'dob'],
    )
}}

select
    try_cast(trim(customer_id) as number) as customer_id,
    trim(name) as name,
    upper(trim(country)) as country,
    try_cast(trim(dob) as date) as dob,
    _loaded_at
from {{ source('raw', 'customers_raw') }}
where customer_id is not null

{% endsnapshot %}
