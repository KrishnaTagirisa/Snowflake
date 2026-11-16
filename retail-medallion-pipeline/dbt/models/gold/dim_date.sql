-- Calendar dimension with UK fiscal year (starts 1 April).

with spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="to_date('" ~ var('dim_date_start') ~ "')",
        end_date="dateadd(day, 1, to_date('" ~ var('dim_date_end') ~ "'))"
    ) }}

),

dates as (

    select date_day::date as calendar_date from spine

)

select
    to_number(to_char(calendar_date, 'YYYYMMDD'))                as date_key,
    calendar_date,
    year(calendar_date)                                          as calendar_year,
    quarter(calendar_date)                                       as calendar_quarter,
    month(calendar_date)                                         as month_number,
    monthname(calendar_date)                                     as month_short_name,
    to_char(calendar_date, 'YYYY-MM')                            as year_month,
    weekofyear(calendar_date)                                    as iso_week,
    dayofweekiso(calendar_date)                                  as day_of_week_iso,
    dayname(calendar_date)                                       as day_short_name,
    dayofweekiso(calendar_date) in (6, 7)                        as is_weekend,
    case when month(calendar_date) >= 4
         then year(calendar_date) else year(calendar_date) - 1 end as fiscal_year_start,
    'FY' || right(
        case when month(calendar_date) >= 4
             then year(calendar_date) + 1 else year(calendar_date) end::varchar, 2)
                                                                 as fiscal_year_label,
    calendar_date = last_day(calendar_date)                      as is_month_end
from dates
