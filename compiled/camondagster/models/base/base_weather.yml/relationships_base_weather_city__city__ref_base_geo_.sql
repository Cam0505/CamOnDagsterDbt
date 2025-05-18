
    
    

with child as (
    select city as from_field
    from "camondagster"."public_base"."base_weather"
    where city is not null
),

parent as (
    select city as to_field
    from "camondagster"."public_base"."base_geo"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


