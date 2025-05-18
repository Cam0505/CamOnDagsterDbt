
    
    

with child as (
    select meal_id as from_field
    from "camondagster"."public_base"."base_meal_consumption"
    where meal_id is not null
),

parent as (
    select meal_id as to_field
    from "camondagster"."public_base"."base_meal_category_country"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


