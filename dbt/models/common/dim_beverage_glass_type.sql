-- ------------------------------------------------------------------------------
-- Model: Dim_beverage_glass_type
-- Description: Dimension Table, beverage glass type information
-- ------------------------------------------------------------------------------
-- Change Log:
-- Date       | Author   | Description
-- -----------|----------|-------------------------------------------------------
-- 2025-05-17 | Cam      | Initial creation
-- YYYY-MM-DD | NAME     | [Add future changes here]
-- ------------------------------------------------------------------------------

SELECT glass_type, glass_type_sk
    from {{ref('glass_type_snapshot')}}
	group by glass_type, glass_type_sk
	