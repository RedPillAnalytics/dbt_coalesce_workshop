with source as (

    select * from {{ source('raw', 'school_learning_modalities') }}

),

filtered as (

    select
        district_nces_id,
        district_name,
        to_date(week) as week,
        learning_modality,
        operational_schools,
        student_count,
        city,
        state,
        zip_code
    from source
    where week < to_date('2022-01-01', 'YYYY-MM-DD')

)

select * from filtered
