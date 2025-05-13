select city, latitude, longitude, region, City_SK,
Country_SK
from {{ref('staging_geo')}}
group by city, latitude, longitude, region, City_SK, Country_SK