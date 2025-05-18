-- ------------------------------------------------------------------------------
-- Model: Dim_beverage_type
-- Description: Dimension Table, beverage type information
-- ------------------------------------------------------------------------------
-- Change Log:
-- Date       | Author   | Description
-- -----------|----------|-------------------------------------------------------
-- 2025-05-17 | Cam      | Initial creation
-- YYYY-MM-DD | NAME     | [Add future changes here]
-- ------------------------------------------------------------------------------
select beverage_type, alcoholic_type, beverage_category_sk, Alcoholic_Type_SK
	From {{ref('staging_beverage_lookup')}}
	group by beverage_type, alcoholic_type, beverage_category_sk, Alcoholic_Type_SK