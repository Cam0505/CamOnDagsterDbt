-- ------------------------------------------------------------------------------
-- Model: Base_rm_characters
-- Description: Base Table for ricky and morty characters from API
-- ------------------------------------------------------------------------------
-- Change Log:
-- Date       | Author   | Description
-- -----------|----------|-------------------------------------------------------
-- 2025-05-16 | Cam      | Initial creation
-- YYYY-MM-DD | NAME     | [Add future changes here]
-- ------------------------------------------------------------------------------
SELECT id, name, status, species, gender, origin__name, location__name, location__url, 
image, url, created AT TIME ZONE 'UTC' AT TIME ZONE 'Australia/Melbourne' AS created
, "_dlt_id" as character_dlt_id
FROM "camondagster"."rick_and_morty_data"."character"