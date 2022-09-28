{{
    config(
        materialized = "incremental",
        unique_key = "row_key"
    )
}}

with
slm_2021    as ( select * from {{ ref('stg_raw_school_learning_modalities') }} ),
curr        as ( select * from {{ ref('stg_sftp_school_learning_modalities') }} ),

final as (
    
    select
        md5(district_nces_id || week) as row_key,
        district_nces_id,
        district_name,
        week,
        learning_modality,
        operational_schools,
        student_count,
        city,
        state,
        zip_code
    from slm_2021
    
    union all

    select
        md5(district_nces_id || week) as row_key,
        district_nces_id,
        district_name,
        week,
        learning_modality,
        operational_schools,
        student_count,
        city,
        state,
        zip_code
    from curr

)

select * from final
