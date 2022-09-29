

with
slm as (select * from {{ ref('int_school_learning_modalities') }}),

final as (
    
    select
        *
    from slm

)

select * from final
