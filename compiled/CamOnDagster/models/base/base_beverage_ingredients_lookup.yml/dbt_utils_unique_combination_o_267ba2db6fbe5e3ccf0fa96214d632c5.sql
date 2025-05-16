





with validation_errors as (

    select
        ingredient, beverage_id
    from "my_duckdb"."main_base"."base_beverage_ingredients_lookup"
    group by ingredient, beverage_id
    having count(*) > 1

)

select *
from validation_errors


