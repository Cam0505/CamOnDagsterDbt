-- ------------------------------------------------------------------------------
-- Model: Staging_Meal_Consumption
-- Description: Fact Table data, consumption events generated from API 
-- ------------------------------------------------------------------------------
-- Change Log:
-- Date       | Author   | Description
-- -----------|----------|-------------------------------------------------------
-- 2025-05-12 | Cam      | Initial creation
-- YYYY-MM-DD | NAME     | [Add future changes here]
-- ------------------------------------------------------------------------------

SELECT bc.meal_id as meal_id, bc.meal_name as meal_name,
bc.category_name as category_name, bc.country_name as country_name,
-- Dim SK
bcl.meal_country_category_sk as meal_country_category_sk,

bc.instructions as instructions, bc.meal_image_url as meal_image_url, bc.meal_youtube_url as meal_youtube_url, 
str_ingredient1, str_ingredient2, str_ingredient3, str_ingredient4, str_ingredient5, str_ingredient6, 
str_ingredient7, str_ingredient8, str_ingredient9, str_ingredient10, str_ingredient11, str_ingredient12, 
str_ingredient13, str_ingredient14, str_ingredient15, str_ingredient16, str_ingredient17, str_ingredient18, 
str_ingredient19, str_ingredient20, bc.meal_url as meal_url
	-- FROM public_base.base_beverage_consumption as bc
    FROM "camondagster"."public_base"."base_meal_consumption" as bc
	left join "camondagster"."public_staging"."staging_meal_category_lookup"  as bcl
	on bc.meal_id = bcl.meal_id