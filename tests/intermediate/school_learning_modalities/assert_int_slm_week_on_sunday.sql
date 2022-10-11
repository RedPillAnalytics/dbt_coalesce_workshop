{{-
    config(
        tags=['inspection_false']
    )
-}}

with 
source          as (select * from {{ ref('int_school_learning_modalities_all') }}),
calendar_day    as (select * from {{ ref('int_conformed_calendar_day') }}),
calendar_week   as (select * from {{ ref('int_conformed_calendar_week') }}),

final as (

    select
        fivetran_file,
        fivetran_line,
        district_nces_id,
        week,
        week as rejected_value,
        'week provided is not the expected week start date of ' || calendar_day.week_start_date as rejected_reason
    from source
    left outer join calendar_day on
        source.week = calendar_day.date_day
    where not exists (
        select 1
        from calendar_week
        where source.week = calendar_week.week_start_date
    )

)

select * from final
