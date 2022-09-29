{{
    config(
        materialized = "table"
    )
}}

with date_dimension as ({{ dbt_date.get_date_dimension( var("dbt_date:start_date"), var("dbt_date:end_date") ) }})

select * from date_dimension
