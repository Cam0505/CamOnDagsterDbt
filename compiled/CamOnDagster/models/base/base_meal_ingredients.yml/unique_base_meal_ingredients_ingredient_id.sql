
    
    

select
    ingredient_id as unique_field,
    count(*) as n_records

from "my_duckdb"."main_base"."base_meal_ingredients"
where ingredient_id is not null
group by ingredient_id
having count(*) > 1


