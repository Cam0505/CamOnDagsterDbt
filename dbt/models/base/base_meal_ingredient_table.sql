-- ------------------------------------------------------------------------------
-- Model: Base_meal_ingredient_table
-- Description: Base Table for list of meals for each ingredient
-- ------------------------------------------------------------------------------
-- Change Log:
-- Date       | Author   | Description
-- -----------|----------|-------------------------------------------------------
-- 2025-05-12 | Cam      | Initial creation
-- YYYY-MM-DD | NAME     | [Add future changes here]
-- ------------------------------------------------------------------------------

SELECT str_meal as meal_name, id_meal as meal_id, 
source_ingredient as ingredient_name
FROM {{ source("meals", "ingredient_table") }} 