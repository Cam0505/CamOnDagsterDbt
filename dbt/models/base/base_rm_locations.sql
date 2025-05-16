
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

SELECT id as location_id, name as location_name, type as location_type, 
dimension as location_dimension, url as location_url
,created AT TIME ZONE 'UTC' AT TIME ZONE 'Australia/Melbourne' AS location_created
,"_dlt_id" as location_dlt_id
FROM {{ source("rick_and_morty", "location") }}