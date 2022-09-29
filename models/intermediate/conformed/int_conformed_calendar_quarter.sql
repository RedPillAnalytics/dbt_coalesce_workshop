{{
    config(
        materialized = "table"
    )
}}

with periods as (
    {{ dbt_date.get_base_dates(var("dbt_date:start_date"), var("dbt_date:end_date"), n_dateparts=12, datepart="quarter") }}
),

final as (

    select
        periods.date_quarter,
        cast({{ dbt_date.date_part('quarter', 'periods.date_quarter') }} as {{ type_int() }}) as quarter_of_year,
        cast({{ date_trunc('quarter', 'periods.date_quarter') }} as date) as quarter_start_date,
        cast({{ last_day('periods.date_quarter', 'quarter') }} as date) as quarter_end_date
    from periods

)

select * from final
