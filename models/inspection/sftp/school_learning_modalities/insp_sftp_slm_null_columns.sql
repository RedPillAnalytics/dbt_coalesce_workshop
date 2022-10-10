with 
source as (select * from {{ ref('stg_sftp_school_learning_modalities') }}),

{# Get all the columns -#}
{%- set cols = ['district_nces_id', 'district_name', 'week','learning_modality', 'operational_schools', 'student_count', 'city', 'state', 'zip_code'] -%}

final as (

{%- for col in cols %}

{%- set reject_reason = "'" ~ col ~ " column cannot be null" ~ "'" %}

    select
        fivetran_file,
        fivetran_line,
        district_nces_id,
        week,
        '{{col}}' as rejected_value,
        {{reject_reason}} as rejected_reason
    from source
    where {{col}} is null

    {% if not loop.last %}union all{% endif %}
{%- endfor -%}

)

select * from final
