-- ------------------------------------------------------------------------------
-- Model: Base_rm_episode_character
-- Description: Base join table Testing GitHub Actions
-- ------------------------------------------------------------------------------
-- Change Log:
-- Date       | Author   | Description
-- -----------|----------|-------------------------------------------------------
-- 2025-05-16 | Cam      | Initial creation
-- YYYY-MM-DD | NAME     | [Add future changes here]
-- ------------------------------------------------------------------------------

SELECT
  _dlt_root_id AS episode_dlt_id,
  CAST(regexp_replace(value, '.*/(\d+)$', '\1') AS INTEGER) AS character_id
FROM {{ source("rick_and_morty", "episode__characters") }}