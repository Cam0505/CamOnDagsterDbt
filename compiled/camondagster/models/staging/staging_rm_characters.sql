-- ------------------------------------------------------------------------------
-- Model: Staging_rm_characters
-- Description: Staging model for characters data
-- ------------------------------------------------------------------------------
-- Change Log:
-- Date       | Author   | Description
-- -----------|----------|-------------------------------------------------------
-- 2025-05-19 | Cam      | Initial creation
-- YYYY-MM-DD | NAME     | [Add future changes here]
-- ------------------------------------------------------------------------------
with episodes_per_character as (
  
(
    SELECT
        
            character_dlt_id
        ,
        COUNT(episode_id) AS Num_Episodes
    FROM "camondagster"."public_base"."base_rm_character_episode"
    GROUP BY
        
            character_dlt_id
        
)
 
)

SELECT bc.character_id, character_name, 
CASE
    WHEN regexp_matches(character_name, '^Rick\b', 'i') THEN 'Rick'
    WHEN regexp_matches(character_name, '^Morty\b', 'i') THEN 'Morty'
    WHEN regexp_matches(character_name, '^Summer\b', 'i') THEN 'Summer'
    WHEN regexp_matches(character_name, '^Beth\b', 'i') THEN 'Beth'
    WHEN regexp_matches(character_name, '^Jerry\b', 'i') THEN 'Jerry'
    ELSE 'Other'
  END AS character_group,
character_status, character_species, 
character_gender, character_origin,
character_image, character_created, bc.character_dlt_id, epc.Num_Episodes
FROM "camondagster"."public_base"."base_rm_characters" as bc 
left join episodes_per_character as epc
on bc.character_dlt_id = epc.character_dlt_id
order by bc.character_id