Select it.source_ingredient as Ingredient,
it.id_drink as beverage_id,
it.str_drink as Beverage_Name
from "camondagster"."beverage_data"."ingredients_table"  as it
where it.id_drink is not null