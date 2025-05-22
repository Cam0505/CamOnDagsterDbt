{{
  config(
    materialized = "view"
  )
}}


SELECT date_col
from {{ref('dim_date')}} as t
where date_col <= (current_date  - 1) 
and NOT EXISTS (
    SELECT 1
    from {{ref('base_uv')}} as o
    WHERE o.uv_date = t.date_col
)
order by date_col desc
limit 6