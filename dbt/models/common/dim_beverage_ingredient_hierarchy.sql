SELECT bil.ingredient, bil.beverage_id, bil.beverage_name
	,bil2.ingredient as ingredient2
	,bil3.ingredient as ingredient3
	,bil4.ingredient as ingredient4
	,bil5.ingredient as ingredient5
	,bil6.ingredient as ingredient6
	,bil7.ingredient as ingredient7
	-- FROM public_base.base_beverage_ingredients_lookup as bil
    from {{ref('base_beverage_ingredients_lookup')}} as bil
	-- Second
	-- left join public_base.base_beverage_ingredients_lookup as bil2 
    left join {{ref('base_beverage_ingredients_lookup')}} as bil2
	on bil.beverage_id = bil2.beverage_id and bil.ingredient != bil2.ingredient
	-- Third
	-- left join public_base.base_beverage_ingredients_lookup as bil3
    left join {{ref('base_beverage_ingredients_lookup')}} as bil3
	on bil.beverage_id = bil3.beverage_id and bil3.ingredient not in (bil2.ingredient, bil.ingredient)
	-- Fourth
	-- left join public_base.base_beverage_ingredients_lookup as bil4
    left join {{ref('base_beverage_ingredients_lookup')}} as bil4
	on bil.beverage_id = bil4.beverage_id and bil4.ingredient not in (bil3.ingredient, bil2.ingredient, bil.ingredient)
	-- Fifth
	-- left join public_base.base_beverage_ingredients_lookup as bil5
    left join {{ref('base_beverage_ingredients_lookup')}} as bil5
	on bil.beverage_id = bil5.beverage_id and bil5.ingredient not in (bil4.ingredient ,bil3.ingredient, bil2.ingredient, bil.ingredient)
	-- sixth
	-- left join public_base.base_beverage_ingredients_lookup as bil6
    left join {{ref('base_beverage_ingredients_lookup')}} as bil6
	on bil.beverage_id = bil6.beverage_id and bil6.ingredient not in (bil5.ingredient, bil4.ingredient ,bil3.ingredient, bil2.ingredient, bil.ingredient)
	-- seventh
	-- left join public_base.base_beverage_ingredients_lookup as bil7
    left join {{ref('base_beverage_ingredients_lookup')}} as bil7
	on bil.beverage_id = bil7.beverage_id and bil7.ingredient not in (bil6.ingredient, bil5.ingredient, bil4.ingredient ,bil3.ingredient, bil2.ingredient, bil.ingredient)
-- 	where bil.beverage_id = '17836'
	order by ingredient desc, beverage_name desc, ingredient2 desc