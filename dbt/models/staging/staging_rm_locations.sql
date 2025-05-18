
-- ------------------------------------------------------------------------------
-- Model: Staging_rm_locations
-- Description: Staging model for Rick and Morty locations
-- ------------------------------------------------------------------------------
-- Change Log:
-- Date       | Author   | Description
-- -----------|----------|-------------------------------------------------------
-- 2025-05-19 | Cam      | Initial creation
-- YYYY-MM-DD | NAME     | [Add future changes here]
-- ------------------------------------------------------------------------------

with resident_per_location as (
	{{count_grouped_records(source_table='base_rm_location_residents',
        group_cols=["location_dlt_id"],
        count_col='character_id',
        alias='Num_Characters'
        ) }} 
)

SELECT bl.location_id, location_name, location_type
,CASE
    WHEN LOWER(location_type) IN ('planet', 'asteroid', 'quasar', 'mount', 'elemental rings', 'dwarf planet (celestial dwarf)') THEN 'Celestial Body'
    WHEN LOWER(location_type) IN ('teenyverse', 'miniverse', 'microverse', 'diegesis', 'non-diegetic alternative reality', 'box', 'machine') THEN 'Artificial World'
    WHEN LOWER(location_type) IN ('game', 'dream', 'memory', 'tv', 'consciousness', 'nightmare', 'fantasy town') THEN 'Virtual/Simulated'
    WHEN LOWER(location_type) IN ('space station', 'resort', 'spa', 'base', 'police department', 'customs', 'daycare', 'spacecraft', 'arcade') THEN 'Location Type'
    WHEN LOWER(location_type) IN ('hell', 'reality', 'liquid', 'death star', 'artificially generated world') THEN 'Abstract Realm'
    WHEN LOWER(location_type) IN ('country', 'cluster', 'menagerie', 'convention') THEN 'Social Structure'
    WHEN LOWER(location_type) IN ('quadrant') THEN 'Quadrant/Zone'
    ELSE 'Unknown' END AS location_categories
, location_dimension
,CASE
        WHEN location_dimension ILIKE '%unknown%' THEN 'Unclassified'
        WHEN LOWER(location_dimension) LIKE 'dimension c%' THEN 'Standard Format'
        WHEN location_dimension ILIKE '%Fascist%' THEN 'Fascist Variant'
        WHEN location_dimension ILIKE '%Magic%' OR
             location_dimension ILIKE '%Fantasy%' OR
             location_dimension ILIKE '%Post-Apocalyptic%' OR
             location_dimension ILIKE '%Merged%' OR
             location_dimension ILIKE '%Evil Rick%' OR
             location_dimension ILIKE '%Testicle%' OR
             location_dimension ILIKE '%Eric Stoltz%' OR
             location_dimension ILIKE '%Wasp%' OR
             location_dimension ILIKE '%Phone%' OR
             location_dimension ILIKE '%Spider%' OR
             location_dimension ILIKE '%Pizza%' OR
             location_dimension ILIKE '%Chair%' OR
             location_dimension ILIKE '%Tusk%' OR
             location_dimension ILIKE '%Cromulon%' THEN 'Thematic'
        WHEN location_dimension ILIKE '%Replacement%' OR location_dimension ILIKE '%Cronenberg%' THEN 'Canonical Named'
        ELSE 'Other' end as dimension_group
, location_created
,bl.location_dlt_id
,rpl.Num_Characters
FROM {{ref('base_rm_locations')}} as bl 
left join resident_per_location as rpl
on bl.location_dlt_id = rpl.location_dlt_id