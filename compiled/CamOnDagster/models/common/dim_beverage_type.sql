select beverage_type, alcoholic_type, beverage_category_sk, Alcoholic_Type_SK
	From "my_duckdb"."main_staging"."staging_beverage_lookup"
	group by beverage_type, alcoholic_type, beverage_category_sk, Alcoholic_Type_SK