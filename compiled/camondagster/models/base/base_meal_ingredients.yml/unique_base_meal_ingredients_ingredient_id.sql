
    
    

select
    ingredient_id as unique_field,
    count(*) as n_records

from "camondagster"."public_base"."base_meal_ingredients"
where ingredient_id is not null
group by ingredient_id
having count(*) > 1


