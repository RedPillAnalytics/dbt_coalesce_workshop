with source as (

    select * from {{ ref('stg_sftp_school_learning_modalities') }}

),

final as (

    select 
        fivetran_file,
        fivetran_line,
        district_nces_id,
        week,
        learning_modality as rejected_value,
        'learning_modality is not in (\'In Person\',\'Hybrid\',\'Remote\')' as rejected_reason
    from source
    where learning_modality not in ('In Person','Hybrid','Remote')

)

select * from final
