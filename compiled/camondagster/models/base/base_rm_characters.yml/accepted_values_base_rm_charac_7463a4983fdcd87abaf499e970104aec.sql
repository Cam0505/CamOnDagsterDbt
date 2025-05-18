
    
    

with all_values as (

    select
        character_gender as value_field,
        count(*) as n_records

    from "camondagster"."public_base"."base_rm_characters"
    group by character_gender

)

select *
from all_values
where value_field not in (
    'Genderless','Male','Female','unknown'
)


