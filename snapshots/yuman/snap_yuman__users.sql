-- ==============================================================================
-- SNAPSHOT: Yuman Users History
-- ==============================================================================
-- Source: stg_yuman__users
-- Purpose: Track historical changes of Yuman app users — captures reassignments,
--          email changes, and hard deletes (user removed from Yuman)
-- Strategy: Timestamp — creates a new record whenever updated_at changes
-- Tracked columns: all (via updated_at bump from Yuman)
--
-- Usage:
--   Query current: SELECT * FROM snapshots.snap_yuman__users WHERE dbt_valid_to IS NULL
--   Query history: SELECT * FROM snapshots.snap_yuman__users WHERE user_id = 'XXX' ORDER BY dbt_valid_from
-- ==============================================================================

{% snapshot snap_yuman__users %}

{{
    config(
      unique_key='user_id',
      strategy='timestamp',
      updated_at='updated_at',
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
        manager_id,
        nomad_id,           -- TRACKED via updated_at
        user_name,          -- TRACKED via updated_at
        user_email,         -- TRACKED via updated_at
        user_type,
        user_phone,
        is_manager_as_technician,
        created_at,
        updated_at,
        extracted_at
    from source_table

{% endsnapshot %}
