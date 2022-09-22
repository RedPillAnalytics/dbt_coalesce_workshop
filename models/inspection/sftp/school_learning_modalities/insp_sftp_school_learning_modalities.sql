with 
    district_nces_id_not_null as (select fivetran_file, fivetran_line from {{ ref('insp_sftp_slm_district_nces_id_not_null') }}),
    district_nces_id_numbers_only as (select fivetran_file, fivetran_line from {{ ref('insp_sftp_slm_district_nces_id_numbers_only') }}),
    learning_modality_accepted_values as (select fivetran_file, fivetran_line from {{ ref('insp_sftp_slm_learning_modality_accepted_values') }}),

final as (

    select * from district_nces_id_not_null
    union all
    select * from district_nces_id_numbers_only
    union all
    select * from learning_modality_accepted_values

)

select * from final
