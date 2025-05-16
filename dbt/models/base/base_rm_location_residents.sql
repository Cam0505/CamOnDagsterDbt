


SELECT
  _dlt_root_id AS location_dlt_id,
  CAST(regexp_replace(value, '.*/(\d+)$', '\1') AS INTEGER) AS character_id
FROM {{ source("rick_and_morty", "location__residents") }}