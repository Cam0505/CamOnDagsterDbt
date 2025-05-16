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
SELECT id as character_id, name as character_name, status as character_status, 
species as character_species, gender as character_gender, 
origin__name as character_origin, 
location__name as character_first_location, location__url as character_first_location_url, 
image as character_image, url as character_url, 
created AT TIME ZONE 'UTC' AT TIME ZONE 'Australia/Melbourne' AS character_created
, "_dlt_id" as character_dlt_id
FROM {{ source("rick_and_morty", "character") }}