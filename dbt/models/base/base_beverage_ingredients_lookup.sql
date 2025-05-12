Select str_ingredient1 as Ingredient,
it.id_drink as beverage_id,
it.str_drink as Beverage_Name
from {{ source("beverages", "ingredients") }} as i 
left join {{ source("beverages", "ingredients_table") }}  as it
	on i.str_ingredient1 = it.source_ingredient
where it.id_drink is not null
