-- ------------------------------------------------------------------------------
-- Model: Dim_meal_ingredient_jointable
-- Description: This model creates a jointable of meal ingredients by joining the
-- ------------------------------------------------------------------------------
-- Change Log:
-- Date       | Author   | Description
-- -----------|----------|-------------------------------------------------------
-- 2025-05-17 | Cam      | Initial creation
-- YYYY-MM-DD | NAME     | [Add future changes here]
-- ------------------------------------------------------------------------------
SELECT distinct meal_id
    from {{ref('staging_meal_ingredients_lookup')}} 
    where meal_id is not null