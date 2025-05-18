





with validation_errors as (

    select
        character_dlt_id, episode_id
    from "camondagster"."public_base"."base_rm_character_episode"
    group by character_dlt_id, episode_id
    having count(*) > 1

)

select *
from validation_errors


