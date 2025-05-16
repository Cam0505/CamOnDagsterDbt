select beverage_name, 
	-- used in dim_beverage to connect to consumption
    beverage_id, 
	beverage_type,
	-- used in dim_beverage and dim_beverage_type as the connection
	md5(cast(coalesce(cast(beverage_type as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as beverage_type_sk,
	-- used in dim_alcoholic_type and dim_beverage_type as connection
	md5(cast(coalesce(cast(beverage_type as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(alcoholic_type as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as beverage_category_sk,
	alcoholic_type,
	-- in dim_alcoholic_type encase any future fact tables need to connect directly 
	md5(cast(coalesce(cast(alcoholic_type as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as alcoholic_type_sk
    from "my_duckdb"."main_base"."base_beverages"