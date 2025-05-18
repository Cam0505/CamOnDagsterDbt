SELECT distinct meal_id
    from "camondagster"."public_staging"."staging_meal_ingredients_lookup" 
    where meal_id is not null