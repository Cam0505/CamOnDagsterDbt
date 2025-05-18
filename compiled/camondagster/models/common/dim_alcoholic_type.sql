SELECT alcoholic_type, 
alcoholic_type_sk
	From "camondagster"."public_staging"."staging_beverage_lookup"
group by alcoholic_type, alcoholic_type_sk