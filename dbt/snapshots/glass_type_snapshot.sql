-- ------------------------------------------------------------------------------
-- Model: glass_type_snapshots.sql
-- Description: Track changes in base_beverage_glass_lookup over time
-- ------------------------------------------------------------------------------
-- Change Log:
-- Date       | Author   | Description
-- -----------|----------|-------------------------------------------------------
-- 2025-05-12 | Cam      | Initial creation
-- YYYY-MM-DD | NAME     | [Add future changes here]
-- ------------------------------------------------------------------------------
{% snapshot glass_type_snapshot %}

{{
    config(
        target_schema='public_snapshots',
        unique_key='Glass_Type_SK',
        strategy='check',
        check_cols=['Glass_Type'],
        invalidate_hard_deletes=True
    )
}}

SELECT 
    Glass_Type,
    Glass_Type_SK
FROM {{ref('base_beverage_glass_lookup')}} 

{% endsnapshot %}