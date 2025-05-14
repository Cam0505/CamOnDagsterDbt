SELECT 
    Id, 
    stock, 
    CAST(price AS DECIMAL) AS price,  -- DuckDB doesn't have a money type
    date_time AT TIME ZONE 'UTC' AT TIME ZONE 'Australia/Melbourne' AS date_time,
    ROUND((MAX(price) OVER(PARTITION BY stock) - MIN(price) OVER(PARTITION BY stock)), 2) AS price_spread,
    ROUND((LAST(price) OVER(PARTITION BY stock ORDER BY date_time) - FIRST(price) OVER(PARTITION BY stock)), 2) AS relative_price_movement,
    ROUND((LAST(price) OVER(PARTITION BY stock) - FIRST(price) OVER(PARTITION BY stock)), 2) AS abs_price_movement,
    COUNT(id) OVER(PARTITION BY stock) AS Num_Stock_Entries
FROM {{ source("gsheets", "gsheets_finance") }}