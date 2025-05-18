
    
    

select
    beverage_id as unique_field,
    count(*) as n_records

from "camondagster"."public_snapshots"."glass_type_snapshot"
where beverage_id is not null
group by beverage_id
having count(*) > 1


