with
slm             as (select * from {{ ref('school_learning_modalities_all') }}),
calendar_day    as (select * from {{ ref('int_conformed_calendar_day') }}),

final as (
    select 
        calendar_day.quarter_start_date,
        slm.learning_modality,
        sum(slm.student_count) as num_students
    from slm
    inner join calendar_day on slm.week = calendar_day.date_day
    group by calendar_day.quarter_start_date, slm.learning_modality
)

select * from final
