-- ------------------------------------------------------------------------------
-- Model: Fact_Meal_Consumption
-- Description: Fact Table, consumption events generated from API 
-- ------------------------------------------------------------------------------
-- Change Log:
-- Date       | Author   | Description
-- -----------|----------|-------------------------------------------------------
-- 2025-05-12 | Cam      | Initial creation
-- YYYY-MM-DD | NAME     | [Add future changes here]
-- ------------------------------------------------------------------------------

SELECT meal_id, meal_name,
category_name, country_name,
meal_country_category_sk,
instructions, meal_image_url, meal_youtube_url, 
str_ingredient1, str_ingredient2, str_ingredient3, str_ingredient4, str_ingredient5, str_ingredient6, 
str_ingredient7, str_ingredient8, str_ingredient9, str_ingredient10, str_ingredient11, str_ingredient12, 
str_ingredient13, str_ingredient14, str_ingredient15, str_ingredient16, str_ingredient17, str_ingredient18, 
str_ingredient19, str_ingredient20, meal_url
FROM "camondagster"."public_staging"."staging_meal_consumption"