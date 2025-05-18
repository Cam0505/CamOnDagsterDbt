
    
    

select
    city_sk as unique_field,
    count(*) as n_records

from "camondagster"."public_common"."dim_city"
where city_sk is not null
group by city_sk
having count(*) > 1


