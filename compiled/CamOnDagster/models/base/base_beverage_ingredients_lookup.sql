Select str_ingredient1 as Ingredient,
it.id_drink as beverage_id,
it.str_drink as Beverage_Name
from "camondagster"."beverage_data"."ingredients" as i 
left join "camondagster"."beverage_data"."ingredients_table"  as it
	on i.str_ingredient1 = it.source_ingredient
where it.id_drink is not null