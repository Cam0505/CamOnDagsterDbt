SELECT airline_id, name, icao, callsign, country, active
FROM {{ source("openflights", "airlines") }}