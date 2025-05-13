select country_code, country, Country_SK
from {{ref('staging_geo')}}
group by country_code, country, Country_SK