select city_id, city, latitude, longitude, country_code, country, region, 
{{ dbt_utils.generate_surrogate_key(["city", "country"]) }} as City_SK,
{{ dbt_utils.generate_surrogate_key(["country"]) }} as Country_SK
from {{ref('base_geo')}}