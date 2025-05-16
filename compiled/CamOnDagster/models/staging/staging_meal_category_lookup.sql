-- ------------------------------------------------------------------------------
-- Model: staging_meal_category_lookup
-- Description: Staging Table, lookup for category and country per meal, used for Dim
-- ------------------------------------------------------------------------------
-- Change Log:
-- Date       | Author   | Description
-- -----------|----------|-------------------------------------------------------
-- 2025-05-12 | Cam      | Initial creation
-- YYYY-MM-DD | NAME     | [Add future changes here]
-- ------------------------------------------------------------------------------


SELECT 
	meal_name, 
	meal_id, category_name,  
	-- Used in Dim_Meal_Category and Dim_Meal_Country
	meal_country_category_sk,
	country_name,
	-- In Dim_Meal_Country encase any future fact tables need to connect directly 
	md5(cast(coalesce(cast(country_name as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as meal_country_sk
    From "my_duckdb"."main_base"."base_meal_category_country"