-- SCD2 integrity: a customer may never have two versions valid at the same instant,
-- and exactly one version must be current.
{{ config(tags=['gold']) }}

with ordered as (

    select
        customer_id,
        valid_from,
        valid_to,
        lead(valid_from) over (partition by customer_id order by valid_from) as next_valid_from
    from {{ ref('dim_customer') }}

),

overlaps as (

    select customer_id, 'overlap' as issue
    from ordered
    where next_valid_from is not null
      and valid_to > next_valid_from

),

current_count as (

    select customer_id, 'current_versions <> 1' as issue
    from {{ ref('dim_customer') }}
    group by customer_id
    having count_if(is_current) <> 1

)

select * from overlaps
union all
select * from current_count
