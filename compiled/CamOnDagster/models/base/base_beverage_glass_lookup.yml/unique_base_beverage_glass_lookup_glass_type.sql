
    
    

select
    glass_type as unique_field,
    count(*) as n_records

from "my_duckdb"."main_base"."base_beverage_glass_lookup"
where glass_type is not null
group by glass_type
having count(*) > 1


