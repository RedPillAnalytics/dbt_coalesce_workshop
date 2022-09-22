{{
    config(
        materialized = "table"
    )
}}

with periods as (
    {{ dbt_date.get_base_dates(var("dbt_date:start_date"), var("dbt_date:end_date"), n_dateparts=52, datepart="week") }}
)

select

    {{ dbt_date.iso_week_start('periods.date_week') }} as iso_week_start_date,
    {{ dbt_date.iso_week_end('periods.date_week') }} as iso_week_end_date,
    {{ dbt_date.iso_week_of_year('periods.date_week') }} as iso_week_of_year,

    cast({{ dbt_date.date_part('quarter', 'periods.date_week') }} as {{ type_int() }}) as quarter_of_year,
    cast({{ date_trunc('quarter', 'periods.date_week') }} as date) as quarter_start_date,
    cast({{ last_day('periods.date_week', 'quarter') }} as date) as quarter_end_date,

    cast({{ dbt_date.date_part('year', 'periods.date_week') }} as {{ type_int() }}) as year_number,
    cast({{ date_trunc('year', 'periods.date_week') }} as date) as year_start_date,
    cast({{ last_day('periods.date_week', 'year') }} as date) as year_end_date
from
    periods
