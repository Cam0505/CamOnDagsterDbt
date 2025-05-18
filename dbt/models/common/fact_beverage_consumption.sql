-- ------------------------------------------------------------------------------
    -- Model: Dim Fact Beverage Consumption
-- Description: Fact Table, beverage consumption information
-- ------------------------------------------------------------------------------
-- Change Log:
-- Date       | Author   | Description
-- -----------|----------|-------------------------------------------------------
-- 2025-05-17 | Cam      | Initial creation
-- YYYY-MM-DD | NAME     | [Add future changes here]
-- ------------------------------------------------------------------------------
SELECT beverage_id, beverage_name, glass_type_sk, Beverage_Category_SK,Beverage_Type_SK, 
Alcoholic_Type_SK, Beverage_Type, Alcoholic_type,
beverage_instructions, beverage_url, Glass_Type, date_melbourne,
str_ingredient1, str_ingredient2, 
str_ingredient3, str_ingredient4, str_ingredient5, 
str_ingredient6, str_ingredient7, str_ingredient8
    From {{ref('staging_beverage_consumption')}} 