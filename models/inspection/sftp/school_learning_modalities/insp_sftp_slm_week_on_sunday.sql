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
        quarter as rejected_value,
        'invalid number of weeks provided for the quarter' as rejected_reason
    from source
    left outer join calendar_week 
)