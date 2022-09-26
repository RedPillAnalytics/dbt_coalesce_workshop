with source as (

    select * from {{ ref('stg_sftp_school_learning_modalities') }}

),

count_district_names as (
    select
        district_nces_id,
        district_name,
        date_trunc('quarter', week) as quarter,
        count(1) as cnt
    from
        source
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
        district_nces_id as rejected_value,
        'district_nces_id is not all numbers' as rejected_reason
    from source
    where exists (
        select 1
        from count_district_names
        where 
            source.district_nces_id = count_district_names.district_nces_id

    )

)

select * from final
