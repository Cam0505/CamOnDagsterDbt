-- ------------------------------------------------------------------------------
-- Model: dim_meal_category
-- Description: Dimension Table, containing meal category information
-- ------------------------------------------------------------------------------
-- Change Log:
-- Date       | Author   | Description
-- -----------|----------|-------------------------------------------------------
-- 2025-05-16 | Cam      | Initial creation
-- YYYY-MM-DD | NAME     | [Add future changes here]
-- ------------------------------------------------------------------------------

SELECT category_name, country_name, meal_country_category_sk
	From "camondagster"."public_staging"."staging_meal_category_lookup"
group by category_name, country_name, meal_country_category_sk