with 
    district_name_consistent as (select fivetran_file, fivetran_line from {{ ref('insp_sftp_slm_district_name_consistent') }}),
    district_nces_id_not_null as (select fivetran_file, fivetran_line from {{ ref('insp_sftp_slm_district_nces_id_not_null') }}),
    district_nces_id_numbers_only as (select fivetran_file, fivetran_line from {{ ref('insp_sftp_slm_district_nces_id_numbers_only') }}),
    duplicates as (select fivetran_file, fivetran_line from {{ ref('insp_sftp_slm_duplicates') }}),
    learning_modality_accepted_values as (select fivetran_file, fivetran_line from {{ ref('insp_sftp_slm_learning_modality_accepted_values') }}),
    week_all_in_quarter as (select fivetran_file, fivetran_line from {{ ref('insp_sftp_slm_week_all_in_quarter') }}),

final as (

    select * from district_name_consistent
    union all
    select * from district_nces_id_not_null
    union all
    select * from district_nces_id_numbers_only
    union all
    select * from duplicates
    union all
    select * from learning_modality_accepted_values
    union all
    select * from week_all_in_quarter

)

select * from final
