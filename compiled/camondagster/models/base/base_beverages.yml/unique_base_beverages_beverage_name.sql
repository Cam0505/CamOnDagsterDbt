
    
    

select
    beverage_name as unique_field,
    count(*) as n_records

from "camondagster"."public_base"."base_beverages"
where beverage_name is not null
group by beverage_name
having count(*) > 1


