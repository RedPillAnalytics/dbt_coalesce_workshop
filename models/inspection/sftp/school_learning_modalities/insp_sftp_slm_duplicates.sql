with source as (

    select * from {{ ref('stg_sftp_school_learning_modalities') }}

),

duplicates as (
    
    select
        district_nces_id,
        district_name,
        week,
        learning_modality,
        operational_schools,
        student_count,
        city,
        state,
        zip_code
    from source
    group by
        district_nces_id,
        district_name,
        week,
        learning_modality,
        operational_schools,
        student_count,
        city,
        state,
        zip_code
    having count(1) > 1
)

final as (

    select 
        fivetran_file,
        fivetran_line,
        district_nces_id,
        week,
        object_construct(
            'district_nces_id', district_nces_id,
            'district_name', district_name,
            'week', week
            ) as rejected_value,
        'duplicated row' as rejected_reason
    from source
    inner join duplicates on (
        district_nces_id = district_nces_id
        district_name = district_name
        week = week
    )

)

select * from final
