{{-
    config(
        tags=['inspection_false']
    )
-}}

with 
source as (select * from {{ ref('stg_sftp_school_learning_modalities') }}),

duplicates as (
    
    select
        district_nces_id,
        week
    from source
    group by
        district_nces_id,
        week
    having count(1) > 1

),

final as (

    select 
        fivetran_file,
        fivetran_line,
        district_nces_id,
        week,
        object_construct(
            'district_nces_id', district_nces_id,
            'week', week
            ) as rejected_value,
        'duplicated row' as rejected_reason
    from source
    where exists (
        select 1
        from duplicates
        where 
            source.district_nces_id = duplicates.district_nces_id
            and source.week = duplicates.week
    )

)

select * from final
