with source as (

    select * from {{ ref('stg_school_learning_modalities') }}

),

final as (

    select distinct
        district_nces_id,
        district_name

    from source

)

select * from final
