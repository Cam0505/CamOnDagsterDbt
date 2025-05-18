

SELECT (date)::date as weather_date, city, temperature_max, temperature_min, temperature_mean, precipitation_sum, 
windspeed_max, windgusts_max, sunshine_duration, location__lat, location__lng, timestamp, "_dlt_load_id", "_dlt_id"
FROM {{ source("weather", "daily_weather") }}