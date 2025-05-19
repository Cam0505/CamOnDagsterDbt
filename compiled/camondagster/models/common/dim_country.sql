-- ------------------------------------------------------------------------------
-- Model: Dim Country
-- Description: Dimension Table, country information
-- ------------------------------------------------------------------------------
-- Change Log:
-- Date       | Author   | Description
-- -----------|----------|-------------------------------------------------------
-- 2025-05-17 | Cam      | Initial creation
-- YYYY-MM-DD | NAME     | [Add future changes here]
-- ------------------------------------------------------------------------------
select country_code, country, Country_SK
from "camondagster"."public_staging"."staging_geo"
group by country_code, country, Country_SK