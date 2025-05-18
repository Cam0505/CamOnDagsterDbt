
    
    

select
    character_id as unique_field,
    count(*) as n_records

from "camondagster"."public_base"."base_rm_characters"
where character_id is not null
group by character_id
having count(*) > 1


