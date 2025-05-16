select city, latitude, longitude, region, City_SK,
Country_SK
from "my_duckdb"."main_staging"."staging_geo"
group by city, latitude, longitude, region, City_SK, Country_SK