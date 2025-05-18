-- ------------------------------------------------------------------------------
-- Model: Dim City
-- Description: Dimension Table, city information
-- ------------------------------------------------------------------------------
-- Change Log:
-- Date       | Author   | Description
-- -----------|----------|-------------------------------------------------------
-- 2025-05-17 | Cam      | Initial creation
-- YYYY-MM-DD | NAME     | [Add future changes here]
-- ------------------------------------------------------------------------------
select city, latitude, longitude, region, City_SK,
Country_SK
from {{ref('staging_geo')}}
group by city, latitude, longitude, region, City_SK, Country_SK