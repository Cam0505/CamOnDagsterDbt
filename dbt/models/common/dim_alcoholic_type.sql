SELECT alcoholic_type, 
alcoholic_type_sk
	From {{ref('staging_beverage_lookup')}}
group by alcoholic_type, alcoholic_type_sk