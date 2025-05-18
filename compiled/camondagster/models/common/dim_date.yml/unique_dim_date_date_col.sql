
    
    

select
    date_col as unique_field,
    count(*) as n_records

from "camondagster"."public_common"."dim_date"
where date_col is not null
group by date_col
having count(*) > 1


