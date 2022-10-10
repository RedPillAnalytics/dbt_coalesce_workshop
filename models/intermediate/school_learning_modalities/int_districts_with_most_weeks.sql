{#
    Noticing that not all districts submit all weeks.
    As of writing most of the districts include 57 weeks of data
    so this is finding the highest number of weeks submitted using a rank
    (which is 57 for now) to then only show data for the districts with 57 weeks of data.
    I was hoping this would show a more even student count over time but
    the result wasn't what I was expecting. Scrapping this.
 #}

with
slm as  ( select * from {{ ref('int_school_learning_modalities') }} ),
dnws as ( select * from {{ ref('int_districts_num_weeks_submitted') }} ),
wwd as  ( select * from {{ ref('int_weeks_with_districts') }} ),

districts_to_keep as (
    select
        dnws.district_nces_id
    from
        dnws
    inner join wwd on
        dnws.num_weeks = wwd.num_weeks
    where
        wwd.rnk = 1
),

final as (

    select
        district_nces_id,
        district_name,
        week,
        learning_modality,
        operational_schools,
        student_count,
        city,
        state,
        zip_code
    from 
        slm
    where exists (
        select 1
        from districts_to_keep
        where slm.district_nces_id = districts_to_keep.district_nces_id
    )

)

select * from final
