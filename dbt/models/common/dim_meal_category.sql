SELECT category_name, country_name, meal_country_category_sk
	From {{ref('staging_meal_category_lookup')}}
group by category_name, country_name, meal_country_category_sk