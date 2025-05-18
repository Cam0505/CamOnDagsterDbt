select country_code, country, Country_SK
from "camondagster"."public_staging"."staging_geo"
group by country_code, country, Country_SK