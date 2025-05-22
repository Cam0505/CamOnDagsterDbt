-- ------------------------------------------------------------------------------
-- Model: Base_uv
-- Description: uv data from global api
-- ------------------------------------------------------------------------------
-- Change Log:
-- Date       | Author   | Description
-- -----------|----------|-------------------------------------------------------
-- 2025-05-22 | Cam      | Initial creation
-- YYYY-MM-DD | NAME     | [Add future changes here]
-- ------------------------------------------------------------------------------

SELECT uv_time::date as uv_date, uv, uv_max, uv_time, ozone, city, location__lat, location__lng
From {{ source("uv", "uv_index") }} 
