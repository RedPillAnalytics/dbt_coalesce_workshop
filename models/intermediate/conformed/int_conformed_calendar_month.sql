{{
    config(
        materialized = "table"
    )
}}

with periods as (
    {{ dbt_date.get_base_dates(var("dbt_date:start_date"), var("dbt_date:end_date"), n_dateparts=12, datepart="month") }}
)

select
    periods.date_month,
    cast({{ date_trunc('month', 'periods.date_month') }} as date) as month_start_date,
    cast({{ last_day('periods.date_month', 'month') }} as date) as month_end_date,

    cast({{ dbt_date.date_part('year', 'periods.date_month') }} as {{ type_int() }}) as year_number,
    cast({{ date_trunc('year', 'periods.date_month') }} as date) as year_start_date,
    cast({{ last_day('periods.date_month', 'year') }} as date) as year_end_date
from
    periods
