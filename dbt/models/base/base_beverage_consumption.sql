SELECT idDrink as id_drink, strDrink as str_drink, strCategory as str_category, strAlcoholic as str_alcoholic, 
strGlass as str_glass, strInstructions as str_instructions, 
strDrinkThumb as str_drink_thumb, (dateModified::timestamp AT TIME ZONE 'Australia/Melbourne')::date AS date_Melbourne,
strIngredient1 as str_ingredient1, strIngredient2 as str_ingredient2, 
strIngredient3 as str_ingredient3, strIngredient4 as str_ingredient4, strIngredient5 as str_ingredient5, strIngredient6 as str_ingredient6, 
strIngredient7 as str_ingredient7, strIngredient8 as str_ingredient8
	-- FROM cocktail_data.beverage_fact_data
    from {{ source("beverages", "beverage_fact_data") }} 