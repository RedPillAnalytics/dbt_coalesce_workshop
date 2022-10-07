with 
source as (select * from {{ source('sftp', 'school_learning_modalities') }}),

renamed as (

    select
        _file as fivetran_file,
        _line as fivetran_line,
        _modified as fivetran_modified,
        _fivetran_synced as fivetran_synced,
        district_nces_id,
        district_name,
        to_date(week) as week,
        learning_modality,
        operational_schools,
        {#- nvl(student_count, 0) as student_count, -#}
        student_count,
        city,
        state,
        zip_code

    from source

)

select * from renamed
