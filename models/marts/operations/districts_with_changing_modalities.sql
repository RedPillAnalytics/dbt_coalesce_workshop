

with
slm as (select * from {{ ref('school_learning_modalities') }}),

final as (
    
    select
        *
    from slm

)

select * from final
