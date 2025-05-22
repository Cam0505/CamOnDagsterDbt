SELECT weather_date, city, temperature_max, temperature_min, 
(temperature_max - temperature_min) AS temperature_range,
temperature_mean, precipitation_sum, 
precipitation_sum - AVG(precipitation_sum) OVER (
  PARTITION BY city ORDER BY weather_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
) AS precipitation_fortnightly_anomaly,
windspeed_max, windgusts_max, sunshine_duration, latitude, longitude,
temperature_mean - AVG(temperature_mean) OVER (
  PARTITION BY city ORDER BY weather_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
) AS mean_temp_fortnightly_anomaly,
AVG(temperature_mean) OVER (
  PARTITION BY city ORDER BY weather_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
) AS mean_temp_fortnightly_avg,
temperature_mean - LAG(temperature_mean) OVER (
  PARTITION BY city ORDER BY weather_date
) AS mean_temp_moving_avg
FROM {{ref('base_weather')}} as bw