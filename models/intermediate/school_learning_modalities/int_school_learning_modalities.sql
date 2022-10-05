{{
    config(
        materialized = "incremental",
        unique_key = "row_key"
    )
}}

with
slm         as ( select * from {{ ref('int_school_learning_modalities_all') }} ),
inspection  as ( select * from {{ ref('insp_sftp_slm_main') }} ),

{# Use inspection layer to remove erroneous data #}
current_inspected as (

    select * from slm
    where not exists (
        select 1
        from inspection
        where 
            slm.fivetran_file = inspection.fivetran_file
            and slm.fivetran_line = inspection.fivetran_line
    )

),

final as (
    
    select
        row_key,
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
