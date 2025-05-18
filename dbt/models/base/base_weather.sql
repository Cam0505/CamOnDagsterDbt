
-- ------------------------------------------------------------------------------
-- Model: base_weather
-- Description: Base Table for weather data from API
-- ------------------------------------------------------------------------------
-- Change Log:
-- Date       | Author   | Description
-- -----------|----------|-------------------------------------------------------
-- 2025-05-18 | Cam      | Initial creation
-- YYYY-MM-DD | NAME     | [Add future changes here]
-- ------------------------------------------------------------------------------


SELECT (date)::date as weather_date, city, temperature_max, temperature_min, temperature_mean, precipitation_sum, 
windspeed_max, windgusts_max, sunshine_duration, location__lat as latitude, location__lng as longitude
FROM {{ source("weather", "daily_weather") }}