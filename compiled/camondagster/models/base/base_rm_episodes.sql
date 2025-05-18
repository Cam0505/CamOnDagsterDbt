-- ------------------------------------------------------------------------------
-- Model: base_rm_episode
-- Description: Base Table for ricky and morty episodes from API
-- ------------------------------------------------------------------------------
-- Change Log:
-- Date       | Author   | Description
-- -----------|----------|-------------------------------------------------------
-- 2025-05-16 | Cam      | Initial creation
-- YYYY-MM-DD | NAME     | [Add future changes here]
-- ------------------------------------------------------------------------------
SELECT id as episode_id, name as episode_name
,STRPTIME(air_date, '%B %d, %Y') AS episode_air_date
, episode, url as episode_url
,created AT TIME ZONE 'UTC' AT TIME ZONE 'Australia/Melbourne' AS episode_created 
,"_dlt_id" as episode_dlt_id
FROM "camondagster"."rick_and_morty_data"."episode"