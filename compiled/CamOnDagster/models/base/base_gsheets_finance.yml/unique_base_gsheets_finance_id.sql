
    
    

select
    id as unique_field,
    count(*) as n_records

from "my_duckdb"."main_base"."base_gsheets_finance"
where id is not null
group by id
having count(*) > 1


