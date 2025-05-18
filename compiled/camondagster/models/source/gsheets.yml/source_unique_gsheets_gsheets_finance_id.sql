
    
    

select
    id as unique_field,
    count(*) as n_records

from "camondagster"."google_sheets_data"."gsheets_finance"
where id is not null
group by id
having count(*) > 1


