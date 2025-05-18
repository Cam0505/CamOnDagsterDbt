-- ------------------------------------------------------------------------------
-- Model: Dim_alcoholic_type
-- Description: Dimension Table, alcoholic type information
-- ------------------------------------------------------------------------------
-- Change Log:
-- Date       | Author   | Description
-- -----------|----------|-------------------------------------------------------
-- 2025-05-17 | Cam      | Initial creation
-- 2025-05-19 | Cam      | Added description and change log
-- YYYY-MM-DD | NAME     | [Add future changes here]
-- ------------------------------------------------------------------------------

SELECT alcoholic_type, 
alcoholic_type_sk
	From {{ref('staging_beverage_lookup')}}
group by alcoholic_type, alcoholic_type_sk