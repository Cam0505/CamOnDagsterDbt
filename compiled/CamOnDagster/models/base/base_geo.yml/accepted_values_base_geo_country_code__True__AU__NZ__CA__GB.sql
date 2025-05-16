
    
    

with all_values as (

    select
        country_code as value_field,
        count(*) as n_records

    from "my_duckdb"."main_base"."base_geo"
    group by country_code

)

select *
from all_values
where value_field not in (
    'AU','NZ','CA','GB'
)


