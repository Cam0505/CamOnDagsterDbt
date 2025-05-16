-- ------------------------------------------------------------------------------
-- Model: Base_rm_character_episode
-- Description: Base join table
-- ------------------------------------------------------------------------------
-- Change Log:
-- Date       | Author   | Description
-- -----------|----------|-------------------------------------------------------
-- 2025-05-16 | Cam      | Initial creation
-- YYYY-MM-DD | NAME     | [Add future changes here]
-- ------------------------------------------------------------------------------


SELECT
  _dlt_root_id AS character_dlt_id,
  CAST(regexp_replace(value, '.*/(\d+)$', '\1') AS INTEGER) AS episode_id
FROM {{ source("rick_and_morty", "character__episode") }}