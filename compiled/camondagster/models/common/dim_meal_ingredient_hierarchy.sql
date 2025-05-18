SELECT sil.meal_id, sil.ingredient_name as ingredient1
,sil2.ingredient_name as ingredient2
,sil3.ingredient_name as ingredient3
,sil4.ingredient_name as ingredient4
FROM "camondagster"."public_staging"."staging_meal_ingredients_lookup" as sil
-- Second
left join "camondagster"."public_staging"."staging_meal_ingredients_lookup" as sil2
on sil.meal_id = sil2.meal_id and sil.ingredient_id != sil2.ingredient_id
-- third
left join "camondagster"."public_staging"."staging_meal_ingredients_lookup" as sil3
on sil.meal_id = sil3.meal_id and sil3.ingredient_id not in (sil2.ingredient_id, sil.ingredient_id)
-- Fourth
left join "camondagster"."public_staging"."staging_meal_ingredients_lookup" as sil4
on sil.meal_id = sil4.meal_id and sil4.ingredient_id not in (sil3.ingredient_id, sil2.ingredient_id, sil.ingredient_id)