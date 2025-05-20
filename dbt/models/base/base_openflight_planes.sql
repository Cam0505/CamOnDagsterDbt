SELECT name, 
NULLIF(iata, '\N') as iata,
NULLIF(icao, '\N') as icao
FROM {{ source("openflights", "planes") }}