with source as (

    select * from {{ ref('stg_school_learning_modalities') }}

),

not_null_id as (
    select _file, _line, _modified, _fivetran_synced, district_nces_id, 'district_nces_id is null' as rejected_reason
    from source
    where district_nces_id is null
),

alpha_id as (
    select _file, _line, district_nces_id, 'district_nces_id is not all numbers' as rejected_reason
    from source
    where REGEXP_LIKE(district_nces_id, '.*[^0-9].*')
),

final as (

    select * from not_null_id
    union all
    select * from alpha_id

)

select * from final
