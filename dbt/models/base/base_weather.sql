

SELECT (date)::date as weather_date, city, temperature_max, temperature_min, temperature_mean, precipitation_sum, 
windspeed_max, windgusts_max, sunshine_duration, location__lat as latitude, location__lng as longitude
FROM {{ source("weather", "daily_weather") }}