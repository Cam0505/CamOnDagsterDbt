
    
    

select
    key as unique_field,
    count(*) as n_records

from "my_duckdb"."main_base"."base_openlibrary"
where key is not null
group by key
having count(*) > 1


