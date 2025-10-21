-- ==============================================================================
-- SNAPSHOT: Device Dimension History
-- ==============================================================================
-- Purpose: Track historical changes to device economic model and company assignments
-- Strategy: Check - Only creates new records when tracked columns change
-- Tracked columns: device_economic_model, company_code
-- 
-- Usage:
--   Run: dbt snapshot
--   Query current: SELECT * FROM snapshots.device_snapshot WHERE dbt_valid_to IS NULL
--   Query history: SELECT * FROM snapshots.device_snapshot WHERE device_id = 'XXX' ORDER BY dbt_valid_from
-- ==============================================================================

{% snapshot device_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='device_id',
      strategy='check',
      check_cols=['device_economic_model', 'company_code'],
      invalidate_hard_deletes=True
    )
}}

-- Select ALL columns you want preserved in history
-- Even though we only CHECK changes on 2 columns, we store everything
select
    device_id,
    device_type_id,
    device_iddevice,
    company_id,
    location_id,
    device_code,
    device_name,
    company_code,              -- ✓ TRACKED for changes
    device_brand,
    device_gamme,
    device_category,
    device_economic_model,      -- ✓ TRACKED for changes
    is_active,
    device_location,
    last_installation_date,
    created_at,
    updated_at
from {{ ref('dim_oracle_neshu__device') }}

{% endsnapshot %}