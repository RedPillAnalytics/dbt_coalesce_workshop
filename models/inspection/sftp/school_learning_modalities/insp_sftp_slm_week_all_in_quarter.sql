with source as (

    select * from {{ ref('stg_sftp_school_learning_modalities') }}

),

calendar_week as (
    
    select * from {{ ref('int_conformed_calendar_week') }}

),

source_with_quarter as (

    select
        fivetran_file,
        fivetran_line,
        week,
        date_trunc('quarter', week) as quarter
    from source

),

week_count_of_quarter as (

    select
        calendar_week.iso_week_start_date,
        calendar_week.quarter_start_date,
        source_with_quarter.fivetran_file,
        source_with_quarter.fivetran_line,
        source_with_quarter.week
    from source_with_quarter
    left outer join calendar_week on calendar_week.quarter_start_date = source_with_quarter.quarter


),

final as (

    select
        fivetran_file,
        fivetran_line,
        week as rejected_value,
        'missing week ' || iso_week_start_date as rejected_reason
    from week_count_of_quarter
    where week_count_of_quarter.fivetran_file is null

)

select * from final
