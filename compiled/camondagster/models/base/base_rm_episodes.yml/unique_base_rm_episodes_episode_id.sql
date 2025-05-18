
    
    

select
    episode_id as unique_field,
    count(*) as n_records

from "camondagster"."public_base"."base_rm_episodes"
where episode_id is not null
group by episode_id
having count(*) > 1


