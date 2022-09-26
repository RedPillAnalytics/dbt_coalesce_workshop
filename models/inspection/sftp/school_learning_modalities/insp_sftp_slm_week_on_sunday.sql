with source as (

    select * from {{ ref('stg_sftp_school_learning_modalities') }}

),

calendar_week as (
    
    select * from {{ ref('int_conformed_calendar_week') }}

),

final as (

    select
        fivetran_file,
        fivetran_line,
        district_nces_id,
        week,
        week as rejected_value,
        'week provided is not an iso_week_start_date' as rejected_reason
    from source
    where not exists (
        select 1
        from calendar_week
        where source.week = calendar_week.iso_week_start_date
    )

)

select * from final
