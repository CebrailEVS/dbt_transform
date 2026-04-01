-- ==============================================================================
-- SNAPSHOT: Yuman Storehouses History
-- ==============================================================================
-- Source: stg_yuman__storehouses
-- Purpose: Track historical changes to storehouses — captures name changes
--          and hard deletes (storehouse removed from Yuman API)
-- Strategy: Check on storehouses_name — invalidate_hard_deletes handles deletions
--
-- Usage:
--   Query current: SELECT * FROM snapshots.snap_yuman__storehouses WHERE dbt_valid_to IS NULL
--   Query history: SELECT * FROM snapshots.snap_yuman__storehouses WHERE storehouses_id = 'XXX' ORDER BY dbt_valid_from
-- ==============================================================================

{% snapshot snap_yuman__storehouses %}

{{
    config(
      unique_key='storehouses_id',
      strategy='check',
      check_cols=['storehouses_name'],
      invalidate_hard_deletes=True,
      tags=['yuman']
    )
}}

    with source_table as (
        select *
        from {{ ref('stg_yuman__storehouses') }}
    )

    select
        storehouses_id,
        storehouses_name,       -- TRACKED for changes
        storehouses_address,
        extracted_at
    from source_table

{% endsnapshot %}
