-- ------------------------------------------------------------------------------
-- Model: Staging_Beverage_Lookup
-- Description: Staging model for beverage lookup data
-- ------------------------------------------------------------------------------
-- Change Log:
-- Date       | Author   | Description
-- -----------|----------|-------------------------------------------------------
-- 2025-05-12 | Cam      | Initial creation
-- YYYY-MM-DD | NAME     | [Add future changes here]
-- ------------------------------------------------------------------------------


select beverage_name, 
	-- used in dim_beverage to connect to consumption
    beverage_id, 
	beverage_type,
	-- used in dim_beverage and dim_beverage_type as the connection
	{{ dbt_utils.generate_surrogate_key(["beverage_type"]) }} as beverage_type_sk,
	-- used in dim_alcoholic_type and dim_beverage_type as connection
	{{ dbt_utils.generate_surrogate_key(["beverage_type", "alcoholic_type"]) }} as beverage_category_sk,
	alcoholic_type,
	-- in dim_alcoholic_type encase any future fact tables need to connect directly 
	{{ dbt_utils.generate_surrogate_key(["alcoholic_type"]) }} as alcoholic_type_sk
    from {{ref('base_beverages')}} 