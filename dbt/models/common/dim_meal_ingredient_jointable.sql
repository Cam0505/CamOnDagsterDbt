SELECT distinct meal_id
    from {{ref('staging_meal_ingredients_lookup')}} 
    where meal_id is not null