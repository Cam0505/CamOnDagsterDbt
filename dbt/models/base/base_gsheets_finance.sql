{{ config(
    materialized='incremental',
    unique_key='id'
) }}

-- ------------------------------------------------------------------------------
-- Model: Base Gsheets Finance
-- Description: Base Table for Gsheets Finance
-- ------------------------------------------------------------------------------
-- Change Log:
-- Date       | Author   | Description
-- -----------|----------|-------------------------------------------------------
-- 2025-05-14 | Cam      | Initial creation
-- YYYY-MM-DD | NAME     | [Add future changes here]
-- ------------------------------------------------------------------------------
SELECT 
    Id, 
    stock, 
    CAST(price AS DECIMAL) AS price,
    {{ convert_to_timezone(column_name='datetime') }} AS date_time,
    ROUND((MAX(price) OVER(PARTITION BY stock) - MIN(price) OVER(PARTITION BY stock)), 2) AS price_spread,
    ROUND((LAST(price) OVER(PARTITION BY stock ORDER BY datetime) - FIRST(price) OVER(PARTITION BY stock)), 2) AS relative_price_movement,
    ROUND((LAST(price) OVER(PARTITION BY stock) - FIRST(price) OVER(PARTITION BY stock)), 2) AS abs_price_movement,
    COUNT(id) OVER(PARTITION BY stock) AS Num_Stock_Entries
FROM {{ source("gsheets", "gsheet_finance_data") }} as existing
{% if is_incremental() %}
    where not exists (select 1 from {{ this }} ck where ck.Id = existing.Id)
{% endif %}