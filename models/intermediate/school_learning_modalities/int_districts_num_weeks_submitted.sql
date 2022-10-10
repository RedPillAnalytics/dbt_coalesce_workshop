with
slm as ( select * from {{ ref('int_school_learning_modalities') }} ),

final as (

    select
        district_nces_id,
        count(week) as num_weeks
    from 
        slm
    group by
        district_nces_id

)

select * from final
