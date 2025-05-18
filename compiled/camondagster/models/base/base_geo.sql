SELECT city_id, city, latitude, longitude, country_code, country, region, continent
From "camondagster"."geo_data"."geo_cities" 
where country in ('New Zealand', 'United Kingdom', 'Australia', 'Canada')