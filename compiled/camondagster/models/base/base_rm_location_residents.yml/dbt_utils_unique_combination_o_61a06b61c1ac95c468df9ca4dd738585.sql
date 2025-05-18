





with validation_errors as (

    select
        location_dlt_id, character_id
    from "camondagster"."public_base"."base_rm_location_residents"
    group by location_dlt_id, character_id
    having count(*) > 1

)

select *
from validation_errors


