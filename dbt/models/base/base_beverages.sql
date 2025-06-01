-- ------------------------------------------------------------------------------
-- Model: Base_Beverages
-- Description: Base Table for multiple Dims - Bev Type, Alcoholic Type and Beverage Name
-- ------------------------------------------------------------------------------
-- Change Log:
-- Date       | Author   | Description
-- -----------|----------|-------------------------------------------------------
-- 2025-05-12 | Cam      | Initial creation
-- YYYY-MM-DD | NAME     | [Add future changes here] (Second Test, Please work)
-- ------------------------------------------------------------------------------
select bt.strDrink as beverage_name, 
    bt.idDrink as beverage_id, 
	bt.source_beverage_type as beverage_type,
	{{ dbt_utils.generate_surrogate_key(["source_beverage_type", "source_alcohol_type"]) }} as beverage_category_sk,
	act.source_alcohol_type as alcoholic_type
	from {{ source("beverages", "beverages_table") }} as bt
    left join {{ source("beverages", "alcoholic_table") }} as act 
	on bt.idDrink = act.idDrink