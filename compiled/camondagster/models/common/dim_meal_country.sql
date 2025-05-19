-- ------------------------------------------------------------------------------
-- Model: Dim_meal_country
-- Description: Dimension Table, meal country information
-- ------------------------------------------------------------------------------
-- Change Log:
-- Date       | Author   | Description
-- -----------|----------|-------------------------------------------------------
-- 2025-05-17 | Cam      | Initial creation
-- YYYY-MM-DD | NAME     | [Add future changes here]
-- ------------------------------------------------------------------------------
SELECT country_name, meal_country_sk
	From "camondagster"."public_staging"."staging_meal_category_lookup"
group by country_name, meal_country_sk