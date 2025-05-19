-- ------------------------------------------------------------------------------
-- Model: Dim_beverage_ingredient_jointable
-- Description: Join table for beverage ingredients
-- ------------------------------------------------------------------------------
-- Change Log:
-- Date       | Author   | Description
-- -----------|----------|-------------------------------------------------------
-- 2025-05-17 | Cam      | Initial creation
-- YYYY-MM-DD | NAME     | [Add future changes here]
-- ------------------------------------------------------------------------------
SELECT distinct id_drink as Beverage_ID
    from "camondagster"."beverage_data"."ingredients_table" as it
    where id_drink is not null