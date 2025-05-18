-- ------------------------------------------------------------------------------
-- Model: base_meal_category_country
-- Description: Base Table for multiple Dims - meal category, meal country
-- ------------------------------------------------------------------------------
-- Change Log:
-- Date       | Author   | Description
-- -----------|----------|-------------------------------------------------------
-- 2025-05-12 | Cam      | Initial creation
-- YYYY-MM-DD | NAME     | [Add future changes here]
-- ------------------------------------------------------------------------------


SELECT ct.str_meal as meal_name, ct.id_meal as meal_id, 
	ct.source_country as country_name,
	md5(cast(coalesce(cast(source_country as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(source_category as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as meal_country_category_sk,
	cat.source_category as category_name
FROM "camondagster"."meals_data"."country_table" as ct
	left join "camondagster"."meals_data"."category_table" as cat 
	on ct.id_meal = cat.id_meal