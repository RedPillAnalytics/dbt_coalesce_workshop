with source as (

    select * from {{ ref('stg_sftp_school_learning_modalities') }}

),

final as (

    select
        fivetran_file,
        fivetran_line,
        district_nces_id,
        week,
        district_nces_id as rejected_value,
        'district_nces_id is null' as rejected_reason
    from source
    where district_nces_id is null
    
)

select * from final
