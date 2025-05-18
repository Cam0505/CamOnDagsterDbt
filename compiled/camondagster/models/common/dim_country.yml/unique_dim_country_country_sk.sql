
    
    

select
    country_sk as unique_field,
    count(*) as n_records

from "camondagster"."public_common"."dim_country"
where country_sk is not null
group by country_sk
having count(*) > 1


