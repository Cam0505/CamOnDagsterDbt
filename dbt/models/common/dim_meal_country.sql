SELECT country_name, meal_country_sk
	From {{ref('staging_meal_category_lookup')}}
group by country_name, meal_country_sk