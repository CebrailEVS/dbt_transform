-- ==============================================================================
-- SNAPSHOT: Yuman Users History
-- ==============================================================================
-- Source: stg_yuman__users
-- Purpose: Track historical changes of Yuman app users — captures reassignments,
--          role/sector changes, and hard deletes (user removed from Yuman)
-- Strategy: Check — creates a new record only when tracked columns change
-- Tracked columns: manager_id, nomad_id, user_name, user_type, user_secteur,
--                  is_manager_as_technician
--
-- Usage:
--   Query current: SELECT * FROM snapshots.snap_yuman__users WHERE dbt_valid_to IS NULL
--   Query history: SELECT * FROM snapshots.snap_yuman__users WHERE user_id = 'XXX' ORDER BY dbt_valid_from
-- ==============================================================================

{% snapshot snap_yuman__users %}

{{
    config(
      unique_key='user_id',
      strategy='check',
      check_cols=['manager_id', 'nomad_id', 'user_name', 'user_type', 'user_secteur', 'is_manager_as_technician'],
      invalidate_hard_deletes=True,
      tags=['yuman']
    )
}}

    with source_table as (
        select *
        from {{ ref('stg_yuman__users') }}
    )

    select
        user_id,
        manager_id,         -- TRACKED
        nomad_id,           -- TRACKED
        user_name,          -- TRACKED
        user_email,
        user_type,          -- TRACKED
        user_phone,
        user_secteur,       -- TRACKED
        is_manager_as_technician, -- TRACKED
        created_at,
        updated_at,
        extracted_at
    from source_table

{% endsnapshot %}
