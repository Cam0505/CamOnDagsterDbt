SELECT country_name, meal_country_sk
	From "my_duckdb"."main_staging"."staging_meal_category_lookup"
group by country_name, meal_country_sk