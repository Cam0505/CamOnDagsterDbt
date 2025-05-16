SELECT alcoholic_type, 
alcoholic_type_sk
	From "my_duckdb"."main_staging"."staging_beverage_lookup"
group by alcoholic_type, alcoholic_type_sk