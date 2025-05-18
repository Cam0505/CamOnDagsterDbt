-- ------------------------------------------------------------------------------
-- Model: Staging_rm_episodes
-- Description: Staging model for Rick and Morty episodes
-- ------------------------------------------------------------------------------
-- Change Log:
-- Date       | Author   | Description
-- -----------|----------|-------------------------------------------------------
-- 2025-05-19 | Cam      | Initial creation
-- YYYY-MM-DD | NAME     | [Add future changes here]
-- ------------------------------------------------------------------------------

with characters_in_episodes as (
    {{count_grouped_records(source_table='base_rm_episode_character',
        group_cols=["episode_dlt_id"],
        count_col='character_id',
        alias='Num_Characters'
        ) }} 
)

SELECT episode_id, episode_name, 
CASE
        WHEN episode_name ILIKE '%ricklantis%' OR episode_name ILIKE '%rickmurai%' OR episode_name ILIKE '%pickle rick%' OR episode_name ILIKE '%rickternal%' THEN 'Rick-Focused'
        WHEN episode_name ILIKE '%morty%' OR episode_name ILIKE '%mort%' THEN 'Morty-Focused'
        WHEN episode_name ILIKE '%interdimensional cable%' OR episode_name ILIKE '%rixty minutes%' THEN 'Interdimensional Cable'
        WHEN episode_name ILIKE '%rick%' AND episode_name ILIKE '%mort%' THEN 'Rick & Morty Duo'
        WHEN episode_name ILIKE '%park%' OR episode_name ILIKE '%shaym%' OR episode_name ILIKE '%recall%' OR episode_name ILIKE '%galactica%' OR episode_name ILIKE '%jack%' OR episode_name ILIKE '%tomorty%' OR episode_name ILIKE '%spotless%' THEN 'Pop Culture Reference'
        ELSE 'Other'
    END AS episode_group,
episode_air_date , episode,
CAST(SUBSTR(episode, 2, 2) AS INTEGER) AS season_num,
    CAST(SUBSTR(episode, 5, 2) AS INTEGER) AS episode_num
,be.episode_dlt_id
,cie.Num_Characters
FROM {{ref('base_rm_episodes')}} as be 
left join characters_in_episodes as cie 
on be.episode_dlt_id = cie.episode_dlt_id