-- ------------------------------------------------------------------------------
-- Model: staging_meal_ingredients_lookup
-- Description: Staging Table, lookup for Ingredients for each meal, used for Dim
-- ------------------------------------------------------------------------------
-- Change Log:
-- Date       | Author   | Description
-- -----------|----------|-------------------------------------------------------
-- 2025-05-12 | Cam      | Initial creation
-- YYYY-MM-DD | NAME     | [Add future changes here]
-- ------------------------------------------------------------------------------

SELECT mit.meal_name, mit.meal_id, mit.ingredient_name, mi.ingredient_id
	FROM {{ref('base_meal_ingredient_table')}} as mit 
	left join {{ref('base_meal_ingredients')}} as mi 
	on mit.ingredient_name = mi.ingredient_name