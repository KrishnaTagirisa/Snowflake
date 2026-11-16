-- Pre-aggregated daily sales mart for Power BI (Import mode friendly).

select
    f.order_date_key,
    d.calendar_date,
    d.fiscal_year_label,
    d.year_month,
    p.category,
    f.sales_channel,
    f.currency,
    count(distinct f.order_id)                                     as orders,
    count(distinct f.customer_id)                                  as customers,
    sum(f.quantity)                                                as units_sold,
    sum(f.gross_amount)                                            as gross_revenue,
    sum(f.discount_amount)                                         as discounts,
    sum(f.net_amount)                                              as net_revenue,
    round(sum(f.net_amount) / nullif(count(distinct f.order_id), 0), 2) as avg_order_value
from {{ ref('fct_order_lines') }} f
inner join {{ ref('dim_date') }} d     on d.date_key = f.order_date_key
inner join {{ ref('dim_product') }} p  on p.product_sk = f.product_sk
where not f.is_cancelled_or_returned
group by all
