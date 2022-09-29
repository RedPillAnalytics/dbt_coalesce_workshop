with
source          as (select * from {{ ref('stg_sftp_school_learning_modalities') }}),
calendar_week   as (select * from {{ ref('int_conformed_calendar_week') }}),

source_with_quarter as (

    select
        fivetran_file,
        fivetran_line,
        district_nces_id,
        district_name,
        week,
        date_trunc('quarter', week) as quarter
    from source

),

week_count_of_quarter as (

    select
        district_nces_id,
        quarter,
        count(1) as num_weeks
    from source_with_quarter
    group by
        district_nces_id,
        quarter

),

week_count_in_quarter as (
    
    select
        quarter_start_date,
        count(1) as num_weeks
    from calendar_week
    group by
        quarter_start_date

),

compare_week_count as (

    select
        week_count_of_quarter.district_nces_id,
        week_count_in_quarter.quarter_start_date,
        week_count_in_quarter.num_weeks as expected_num_weeks,
        week_count_of_quarter.num_weeks as provided_weeks
    from week_count_of_quarter
    left outer join week_count_in_quarter on week_count_of_quarter.quarter = week_count_in_quarter.quarter_start_date

),

final as (

    select
        fivetran_file,
        fivetran_line,
        district_nces_id,
        week,
        quarter as rejected_value,
        'invalid number of weeks provided for the quarter' as rejected_reason
    from source_with_quarter
    where exists (
        select 1
        from compare_week_count
        where
            source_with_quarter.district_nces_id = compare_week_count.district_nces_id
            and source_with_quarter.quarter = compare_week_count.quarter_start_date
    )

)

select * from final
