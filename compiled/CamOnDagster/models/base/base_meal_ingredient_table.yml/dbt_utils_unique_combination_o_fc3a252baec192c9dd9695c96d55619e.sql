





with validation_errors as (

    select
        ingredient_name, meal_id
    from "my_duckdb"."main_base"."base_meal_ingredient_table"
    group by ingredient_name, meal_id
    having count(*) > 1

)

select *
from validation_errors


