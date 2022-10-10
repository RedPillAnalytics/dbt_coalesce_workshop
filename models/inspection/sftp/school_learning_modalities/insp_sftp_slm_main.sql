{{ dbt_utils.union_relations(
    relations=[
        ref('insp_sftp_slm_district_name_consistent'),
        ref('insp_sftp_slm_district_nces_id_numbers_only'),
        ref('insp_sftp_slm_duplicates'),
        ref('insp_sftp_slm_learning_modality_accepted_values'),
        ref('insp_sftp_slm_null_columns'),
        ref('insp_sftp_slm_week_on_sunday')
    ],
) }}
