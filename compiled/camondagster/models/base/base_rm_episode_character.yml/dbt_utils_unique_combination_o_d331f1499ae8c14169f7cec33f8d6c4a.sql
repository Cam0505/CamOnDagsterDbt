





with validation_errors as (

    select
        episode_dlt_id, character_id
    from "camondagster"."public_base"."base_rm_episode_character"
    group by episode_dlt_id, character_id
    having count(*) > 1

)

select *
from validation_errors


