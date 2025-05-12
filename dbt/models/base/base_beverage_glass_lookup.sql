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
{{ dbt_utils.generate_surrogate_key(["str_glass"]) }} as Glass_Type_SK
from {{ source("beverages", "glasses") }}