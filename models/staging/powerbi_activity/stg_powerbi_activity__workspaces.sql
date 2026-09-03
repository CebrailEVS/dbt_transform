{{
    config(
        materialized='table',
        description="Espaces de travail Power BI nettoyés — 1 ligne = 1 espace PARTAGÉ ACTIF, PK workspace_id. Source : powerbi_workspaces. FILTRE OBLIGATOIRE appliqué : type = 'Workspace' and state = 'Active' — écarte les 136 espaces personnels (types PersonalGroup ET Personal) et les 2 espaces supprimés. Attendu : 13 espaces au 2026-09-03 ; s'il en sort 149, le filtre ne s'applique plus."
    )
}}

with source_data as (
    select * from {{ source('powerbi_activity', 'powerbi_workspaces') }}
),

cleaned_data as (
    select
        -- primary key
        id as workspace_id,

        -- attributs (préfixés : `name` / `description` sont des noms génériques
        -- co-joints en aval avec les rapports et les modèles sémantiques,
        -- cf. staging.md § 3 règle 3)
        name as workspace_name,
        description as workspace_description,

        -- classification (constants après filtre — conservés comme preuve du périmètre)
        type,
        state,

        -- capacity / settings
        is_read_only,
        is_on_dedicated_capacity,
        capacity_migration_status,
        has_workspace_level_settings,

        -- dlt lineage
        _dlt_id,

        -- system columns
        -- La source n'expose AUCUNE date métier (ni création ni modification) :
        -- c'est un snapshot `replace` de l'inventaire. created_at / updated_at
        -- sont donc null par contrat, et non par oubli. `state = 'Deleted'`
        -- existe mais sans horodatage → deleted_at reste null.
        cast(null as timestamp) as created_at,
        cast(null as timestamp) as updated_at,
        timestamp(_extracted_at) as extracted_at,
        cast(null as timestamp) as deleted_at

    from source_data
    -- ⚠️ FILTRE OBLIGATOIRE n°3 (cf. docs/architecture/powerbi_activity.md)
    where
        type = 'Workspace'
        and state = 'Active'
)

select * from cleaned_data
