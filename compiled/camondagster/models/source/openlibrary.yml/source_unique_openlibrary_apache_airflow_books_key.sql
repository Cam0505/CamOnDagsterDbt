
    
    

select
    key as unique_field,
    count(*) as n_records

from "camondagster"."openlibrary_data"."apache_airflow_books"
where key is not null
group by key
having count(*) > 1


