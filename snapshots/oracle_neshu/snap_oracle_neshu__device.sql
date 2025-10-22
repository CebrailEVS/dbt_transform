-- ==============================================================================
-- SNAPSHOT: Oracle Neshu Device Dimension History
-- ==============================================================================
-- Source: dim_oracle_neshu__device
-- Purpose: Track historical changes des label : modele economique, localisation, company, marque
-- Strategy: Check - Only creates new records when tracked columns change
-- Tracked columns: device_economic_model, company_code
-- 
-- Usage:
--   Query current: SELECT * FROM snapshots.snap_oracle_neshu__device WHERE dbt_valid_to IS NULL
--   Query history: SELECT * FROM snapshots.snap_oracle_neshu__device WHERE device_id = 'XXX' ORDER BY dbt_valid_from
-- ==============================================================================

{% snapshot snap_oracle_neshu__device %}

{{
    config(
      target_schema='snapshots',
      unique_key='device_id',
      strategy='check',
      check_cols=['device_economic_model', 'device_brand', 'company_code','device_location'],
      invalidate_hard_deletes=True,
      tags=['oracle_neshu']
    )
}}

WITH source_table AS (
  SELECT *
  FROM {{ ref('dim_oracle_neshu__device') }}
)
SELECT
    device_id,
    device_iddevice,
    device_type_id,
    company_id,
    location_id,
    device_code,
    device_name,
    company_code,              -- ✓ TRACKED for changes
    company_name,
    device_brand,              -- ✓ TRACKED for changes
    device_gamme,
    device_category,
    device_economic_model,      -- ✓ TRACKED for changes
    device_location,            -- ✓ TRACKED for changes
    is_active,
    last_installation_date,
    created_at,
    updated_at
FROM source_table

{% endsnapshot %}