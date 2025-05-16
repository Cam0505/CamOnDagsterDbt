SELECT category_name, country_name, meal_country_category_sk
	From "my_duckdb"."main_staging"."staging_meal_category_lookup"
group by category_name, country_name, meal_country_category_sk