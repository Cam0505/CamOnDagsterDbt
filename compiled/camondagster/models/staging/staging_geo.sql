-- ------------------------------------------------------------------------------
-- Model: Staging_geo
-- Description: Staging model for geographic data
-- ------------------------------------------------------------------------------
-- Change Log:
-- Date       | Author   | Description
-- -----------|----------|-------------------------------------------------------
-- 2025-05-19 | Cam      | Initial creation
-- YYYY-MM-DD | NAME     | [Add future changes here]
-- ------------------------------------------------------------------------------
select city_id, city, latitude, longitude, country_code, country, region, 
md5(cast(coalesce(cast(city as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(country as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as City_SK,
md5(cast(coalesce(cast(country as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as Country_SK
from "camondagster"."public_base"."base_geo"