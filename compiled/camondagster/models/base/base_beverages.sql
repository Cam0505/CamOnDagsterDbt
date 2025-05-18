-- ------------------------------------------------------------------------------
-- Model: Base_Beverages
-- Description: Base Table for multiple Dims - Bev Type, Alcoholic Type and Beverage Name
-- ------------------------------------------------------------------------------
-- Change Log:
-- Date       | Author   | Description
-- -----------|----------|-------------------------------------------------------
-- 2025-05-12 | Cam      | Initial creation
-- YYYY-MM-DD | NAME     | [Add future changes here]
-- ------------------------------------------------------------------------------
select bt.str_drink as beverage_name, 
    bt.id_drink as beverage_id, 
	bt.source_beverage_type as beverage_type,
	md5(cast(coalesce(cast(source_beverage_type as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(source_alcohol_type as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as beverage_category_sk,
	act.source_alcohol_type as alcoholic_type
	from "camondagster"."beverage_data"."beverages_table" as bt
    left join "camondagster"."beverage_data"."alcoholic_table" as act 
	on bt.id_drink = act.id_drink