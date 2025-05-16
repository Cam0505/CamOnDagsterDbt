
    
    

select
    meal_id as unique_field,
    count(*) as n_records

from "my_duckdb"."main_base"."base_meal_category_country"
where meal_id is not null
group by meal_id
having count(*) > 1


