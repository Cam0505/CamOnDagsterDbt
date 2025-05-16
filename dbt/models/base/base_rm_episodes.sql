
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
SELECT id, name
,STRPTIME(air_date, '%B %d, %Y') AS air_date
, episode, url 
,created AT TIME ZONE 'UTC' AT TIME ZONE 'Australia/Melbourne' AS created
,"_dlt_id" as episode_dlt_id
FROM {{ source("rick_and_morty", "episode") }}