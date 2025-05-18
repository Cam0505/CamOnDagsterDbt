select city, latitude, longitude, region, City_SK,
Country_SK
from "camondagster"."public_staging"."staging_geo"
group by city, latitude, longitude, region, City_SK, Country_SK