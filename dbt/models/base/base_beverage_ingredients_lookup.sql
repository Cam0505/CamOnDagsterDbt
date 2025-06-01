-- ------------------------------------------------------------------------------
-- Model: Base_beverage_ingredients_lookup
-- Description: Base Table for Beverage Ingredients
-- ------------------------------------------------------------------------------
-- Change Log:
-- Date       | Author   | Description
-- -----------|----------|-------------------------------------------------------
-- 2025-05-12 | Cam      | Initial creation
-- 2025-05-12 | Cam      | Added logic to filter out null beverage IDs
-- YYYY-MM-DD | NAME     | [Add future changes here]
-- ------------------------------------------------------------------------------
Select it.source_ingredient as Ingredient,
it.idDrink as beverage_id,
it.strDrink as Beverage_Name
from {{ source("beverages", "ingredients_table") }}  as it
where it.idDrink is not null
