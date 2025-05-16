
    
    

select
    beverage_id as unique_field,
    count(*) as n_records

from "my_duckdb"."main_base"."base_beverages"
where beverage_id is not null
group by beverage_id
having count(*) > 1


