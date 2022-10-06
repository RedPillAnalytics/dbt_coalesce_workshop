{{
    config(
        materialized = "table"
    )
}}

with
slm as ( select * from {{ ref('stg_sftp_school_learning_modalities') }} ),

final as (

    select
        md5(district_nces_id || week) as row_key,
        fivetran_file,
        fivetran_line,
        district_nces_id,
        district_name,
        week,
        learning_modality,
        operational_schools,
        student_count,
        city,
        state,
        zip_code
    from slm

)

select * from final
