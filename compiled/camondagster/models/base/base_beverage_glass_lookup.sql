-- ------------------------------------------------------------------------------
-- Model: Base_Beverage_glass_lookup
-- Description: Beverage Glass Type Dim Base Table
-- ------------------------------------------------------------------------------
-- Change Log:
-- Date       | Author   | Description
-- -----------|----------|-------------------------------------------------------
-- 2025-05-12 | Cam      | Initial creation
-- YYYY-MM-DD | NAME     | [Add future changes here]
-- ------------------------------------------------------------------------------

Select str_glass as Glass_Type,
md5(cast(coalesce(cast(str_glass as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as Glass_Type_SK
from "camondagster"."beverage_data"."glasses"