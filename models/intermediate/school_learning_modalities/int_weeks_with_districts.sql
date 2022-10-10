with
district_weeks as ( select * from {{ ref('int_districts_num_weeks_submitted') }} ),

top_weeks as (
    select
        num_weeks,
        count(district_nces_id) as num_districts
    from
        district_weeks
    group by num_weeks

),

final as (

    select 
        num_weeks,
        num_districts,
        rank() over (order by num_districts desc) as rnk from top_weeks 
)

select * from final
