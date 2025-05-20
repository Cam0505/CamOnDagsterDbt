SELECT airline, 
NULLIF(airline_id, '\N') as airline_id, source_airport, 
NULLIF(source_airport_id, '\N') as source_airport_id,
NULLIF(destination_airport, '\N') as destination_airport, 
NULLIF(destination_airport_id, '\N') as destination_airport_id, 
stops::int as stops, equipment
from {{ source("openflights", "routes") }}
