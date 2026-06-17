-- ==============================================================================
-- SNAPSHOT: LCDP Device Dimension History
-- ==============================================================================
-- Source: dim_lcdp__device
-- Purpose: Track historical changes du parc machines LCDP (labels, rattachement
--          client/emplacement, statut, etc.).
-- Strategy: Check (check_cols='all') - une nouvelle version uniquement quand une
--           des colonnes métier sélectionnées change réellement (indépendant de
--           updated_at). Pas de version "vide" sur simple bump de modification_date.
--           updated_at est volontairement EXCLU du select (sinon il déclencherait
--           une version à chaque modif ERP, ré-introduisant le bruit).
--           dbt_valid_from = heure d'exécution du snapshot (pas la date métier).
--
-- Usage:
--   Etat courant : SELECT * FROM snapshots.snap_lcdp__device WHERE dbt_valid_to IS NULL
--   Historique   : SELECT * FROM snapshots.snap_lcdp__device WHERE device_id = XXX ORDER BY dbt_valid_from
--   Changements d'un label (ex. device_category) :
--     with history as (
--         select device_id, device_category, dbt_valid_from,
--             lag(device_category) over (partition by device_id order by dbt_valid_from) as prev
--         from snapshots.snap_lcdp__device
--     )
--     select * from history
--     where device_category is distinct from prev and prev is not null
-- ==============================================================================

{% snapshot snap_lcdp__device %}

{{
    config(
      unique_key='device_id',
      strategy='check',
      check_cols='all',
      invalidate_hard_deletes=True,
      tags=['oracle_lcdp']
    )
}}

    with source_table as (
        select *
        from {{ ref('dim_lcdp__device') }}
    )

    select
        -- Identifiants
        device_id,
        device_iddevice,
        device_type_id,
        company_id,
        location_id,

        -- Codes et noms
        device_code,
        device_name,
        company_code,
        company_name,

        -- Caractéristiques machine (labels)
        device_category,
        device_brand,
        device_state,
        device_material_status,
        audit_type,
        typology_da,
        currency_mode,

        -- Types machine (labels)
        fountain_type,
        grinder_type,
        percolator_type,
        type_sp,
        type_dasa,
        model_sp,
        brand_sp,
        badge,

        -- Localisation
        device_location,

        -- Statut
        is_active,

        -- Date de création (immuable, ne déclenche pas de version)
        last_installation_date,
        created_at
    from source_table

{% endsnapshot %}
