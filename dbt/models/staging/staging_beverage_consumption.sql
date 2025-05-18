-- ------------------------------------------------------------------------------
-- Model: Staging_Beverage_Consumption
-- Description: Fact Table data, consumption events generated from API 
-- ------------------------------------------------------------------------------
-- Change Log:
-- Date       | Author   | Description
-- -----------|----------|-------------------------------------------------------
-- 2025-05-12 | Cam      | Initial creation
-- YYYY-MM-DD | NAME     | [Add future changes here]
-- ------------------------------------------------------------------------------

select bc.id_drink as beverage_id, str_drink as beverage_name, 
bcl.beverage_category_sk as beverage_category_sk,
str_glass as glass_type,
bcl.beverage_type_sk as beverage_type_sk,
bcl.beverage_type as beverage_type,
bcl.alcoholic_type_sk as alcoholic_type_sk,
bcl.alcoholic_type as alcoholic_type,
str_category, 
str_alcoholic, 
bgl.glass_type_sk,
str_instructions as beverage_instructions, str_drink_thumb as beverage_url, date_melbourne,
str_ingredient1, str_ingredient2, 
str_ingredient3, str_ingredient4, str_ingredient5, 
str_ingredient6, str_ingredient7, str_ingredient8
	-- from public_base.base_beverage_consumption as bc
    from {{ref('base_beverage_consumption')}} as bc
	-- left join public_base.base_beverage_glass_lookup as bgl
    left join {{ref('glass_type_snapshot')}}  as bgl
	on bc.id_drink = bgl.beverage_id and dbt_valid_to is null
	left join {{ref('staging_beverage_lookup')}}  as bcl
	on bc.id_drink = bcl.beverage_id
	