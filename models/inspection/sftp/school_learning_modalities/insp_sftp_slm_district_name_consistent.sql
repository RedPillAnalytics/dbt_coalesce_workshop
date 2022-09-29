with
source as (select * from {{ ref('stg_sftp_school_learning_modalities') }}),

district_names_by_quarter as (
    select
        district_nces_id,
        district_name,
        date_trunc('quarter', week) as quarter
    from source
    group by
        district_nces_id,
        district_name,
        quarter

),

count_district_names as (

    select
        district_nces_id,
        district_name,
        quarter,
        count(1) as cnt
    from district_names_by_quarter
    group by
        district_nces_id,
        district_name,
        quarter
    having count(1) > 1

),

final as (

    select
        fivetran_file,
        fivetran_line,
        district_nces_id,
        week,
        district_name as rejected_value,
        'district_name changes during the quarter ' || date_trunc('quarter', week) as rejected_reason
    from source
    where exists (
        select 1
        from count_district_names
        where 
            source.district_nces_id = count_district_names.district_nces_id
            and date_trunc('quarter', source.week) = count_district_names.quarter

    )

)

select * from final
