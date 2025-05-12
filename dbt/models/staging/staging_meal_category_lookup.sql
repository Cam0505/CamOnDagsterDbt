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
	{{ dbt_utils.generate_surrogate_key(["country_name"]) }} as meal_country_sk
    From {{ref('base_meal_category_country')}} 