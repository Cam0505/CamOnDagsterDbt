SELECT weather_date, sw.city, temperature_max, temperature_min, temperature_range, 
temperature_mean, precipitation_sum, precipitation_fortnightly_anomaly, windspeed_max, windgusts_max, 
sunshine_duration, mean_temp_fortnightly_anomaly, mean_temp_fortnightly_avg, mean_temp_moving_avg,
sg.City_SK
FROM {{ref('staging_weather')}} as sw
left join {{ref('staging_geo')}} sg 
on sw.city = sg.city