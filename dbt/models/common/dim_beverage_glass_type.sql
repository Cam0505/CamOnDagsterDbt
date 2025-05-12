with base as (
SELECT glass_type, glass_type_sk, dbt_valid_to,
row_number() over(partition by glass_type_sk order by dbt_valid_to desc) as rw_num
    from {{ref('glass_type_snapshot')}}
	)
	
	Select glass_type, glass_type_sk
	from base
	where rw_num = 1