-- ------------------------------------------------------------------------------
-- Model: Dim Date
-- Description: Dimension Table, date information
-- ------------------------------------------------------------------------------
-- Change Log:
-- Date       | Author   | Description
-- -----------|----------|-------------------------------------------------------
-- 2025-05-17 | Cam      | Initial creation
-- YYYY-MM-DD | NAME     | [Add future changes here]
-- ------------------------------------------------------------------------------


WITH RECURSIVE date_series AS (
    SELECT DATE '2000-01-01' AS date
    UNION ALL
    SELECT date + INTERVAL '1 day'
    FROM date_series
    WHERE date + INTERVAL '1 day' < DATE '2030-01-01'
)
SELECT
    date AS date_col,
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(MONTH FROM date) AS month,
    EXTRACT(DAY FROM date) AS day,
    strftime('%B', date) AS month_name,  -- Replacing TO_CHAR with strftime
    strftime('%A', date) AS weekday_name,  -- Replacing TO_CHAR with strftime
    EXTRACT(DOW FROM date) AS day_of_week,
    CASE WHEN EXTRACT(DOW FROM date) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
    EXTRACT(DOY FROM date) AS day_of_year,
    EXTRACT(WEEK FROM date) AS week_of_year,
    EXTRACT(QUARTER FROM date) AS quarter
FROM date_series