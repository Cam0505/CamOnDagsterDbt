
    
    

with all_values as (

    select
        character_status as value_field,
        count(*) as n_records

    from "camondagster"."public_base"."base_rm_characters"
    group by character_status

)

select *
from all_values
where value_field not in (
    'Alive','unknown','Dead'
)


