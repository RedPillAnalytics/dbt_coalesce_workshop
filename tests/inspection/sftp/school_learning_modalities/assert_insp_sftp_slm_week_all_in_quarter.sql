{{
    config(
        enabled = False
    )
}}
{#- Disabling as referenced model is disabled as well -#}

select * from {{ ref('insp_sftp_slm_week_all_in_quarter') }}
