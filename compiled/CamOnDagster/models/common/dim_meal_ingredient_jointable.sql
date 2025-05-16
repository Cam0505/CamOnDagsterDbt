SELECT distinct meal_id
    from "my_duckdb"."main_staging"."staging_meal_ingredients_lookup" 
    where meal_id is not null