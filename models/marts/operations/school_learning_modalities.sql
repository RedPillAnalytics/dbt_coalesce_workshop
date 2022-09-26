{{
    config(
        materialized = "incremental",
        unique_key = "row_key"
    )
}}

with
    slm_2021    as ( select * from {{ ref('stg_raw_school_learning_modalities') }} ),
    curr        as ( select * from {{ ref('stg_sftp_school_learning_modalities') }} ),
    insp        as ( select * from {{ ref('insp_sftp_school_learning_modalities') }} ),

{# Use inspection layer to remove erroneous data #}
current_inspected as (

    select * from curr
    where not exists (
        select 1
        from insp
        where 
            curr.fivetran_file = insp.fivetran_file
            and curr.fivetran_line = insp.fivetran_line
    )

),

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
    from current_inspected

)

select * from final
