-- ------------------------------------------------------------------------------
-- Model: Dim_meal_ingredient_hierarchy
-- Description: This model creates a hierarchy of meal ingredients by joining the
-- ------------------------------------------------------------------------------
-- Change Log:
-- Date       | Author   | Description
-- -----------|----------|-------------------------------------------------------
-- 2025-05-17 | Cam      | Initial creation
-- YYYY-MM-DD | NAME     | [Add future changes here]
-- ------------------------------------------------------------------------------
SELECT sil.meal_id, sil.ingredient_name as ingredient1
,sil2.ingredient_name as ingredient2
,sil3.ingredient_name as ingredient3
,sil4.ingredient_name as ingredient4
FROM {{ref('staging_meal_ingredients_lookup')}} as sil
-- Second
left join {{ref('staging_meal_ingredients_lookup')}} as sil2
on sil.meal_id = sil2.meal_id and sil.ingredient_id != sil2.ingredient_id
-- third
left join {{ref('staging_meal_ingredients_lookup')}} as sil3
on sil.meal_id = sil3.meal_id and sil3.ingredient_id not in (sil2.ingredient_id, sil.ingredient_id)
-- Fourth
left join {{ref('staging_meal_ingredients_lookup')}} as sil4
on sil.meal_id = sil4.meal_id and sil4.ingredient_id not in (sil3.ingredient_id, sil2.ingredient_id, sil.ingredient_id)