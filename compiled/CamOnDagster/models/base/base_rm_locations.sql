-- ------------------------------------------------------------------------------
-- Model: base_rm_locations
-- Description: Base Table for ricky and morty locations from API
-- ------------------------------------------------------------------------------
-- Change Log:
-- Date       | Author   | Description
-- -----------|----------|-------------------------------------------------------
-- 2025-05-16 | Cam      | Initial creation
-- YYYY-MM-DD | NAME     | [Add future changes here]
-- ------------------------------------------------------------------------------

SELECT id, name, type, dimension, url
,created AT TIME ZONE 'UTC' AT TIME ZONE 'Australia/Melbourne' AS created
,"_dlt_id" as location_dlt_id
FROM "camondagster"."rick_and_morty_data"."location"