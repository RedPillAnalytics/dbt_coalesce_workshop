with source as (

    select * from {{ source('sftp', 'school_learning_modalities') }}

),

renamed as (

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

)

select * from renamed
