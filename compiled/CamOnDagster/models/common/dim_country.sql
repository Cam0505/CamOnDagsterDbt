select country_code, country, Country_SK
from "my_duckdb"."main_staging"."staging_geo"
group by country_code, country, Country_SK