-- ------------------------------------------------------------------------------
-- Model: Base_Geo
-- Description: Base Table for multiple Dims - City, Country, Region and Continent
-- ------------------------------------------------------------------------------
-- Change Log:
-- Date       | Author   | Description
-- -----------|----------|-------------------------------------------------------
-- 2025-05-15 | Cam      | Initial creation
-- YYYY-MM-DD | NAME     | [Add future changes here]
-- ------------------------------------------------------------------------------
SELECT city_id, city, latitude, longitude, country_code, country, region, continent
From "camondagster"."geo_data"."geo_cities" 
where country in ('New Zealand', 'United Kingdom', 'Australia', 'Canada')