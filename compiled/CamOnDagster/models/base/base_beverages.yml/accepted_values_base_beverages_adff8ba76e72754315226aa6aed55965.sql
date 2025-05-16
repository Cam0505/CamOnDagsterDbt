
    
    

with all_values as (

    select
        alcoholic_type as value_field,
        count(*) as n_records

    from "my_duckdb"."main_base"."base_beverages"
    group by alcoholic_type

)

select *
from all_values
where value_field not in (
    'Non alcoholic','Optional alcohol','Alcoholic'
)


