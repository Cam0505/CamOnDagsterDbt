
    
    

with child as (
    select id_drink as from_field
    from "my_duckdb"."main_base"."base_beverage_consumption"
    where id_drink is not null
),

parent as (
    select beverage_id as to_field
    from "my_duckdb"."main_base"."base_beverages"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


