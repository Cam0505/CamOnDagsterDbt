
    
    

select
    meal_country_category_sk as unique_field,
    count(*) as n_records

from "camondagster"."public_common"."dim_meal_category"
where meal_country_category_sk is not null
group by meal_country_category_sk
having count(*) > 1


