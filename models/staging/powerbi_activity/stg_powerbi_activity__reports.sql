{{
    config(
        materialized='table',
        cluster_by=['workspace_id', 'dataset_id'],
        description="Rapports Power BI nettoyés — 1 ligne = 1 rapport original hors métriques d'usage, PK report_id. Source : powerbi_reports. DEUX FILTRES OBLIGATOIRES appliqués : app_id is null (écarte les copies d'App, que les événements ViewReport ne référencent jamais) et exclusion des rapports 'usage metrics report' auto-générés. Attendu : 100 rapports au 2026-09-03 ; s'il en sort 139, les filtres ne s'appliquent plus. Le périmètre métier (37 rapports) exige EN PLUS la restriction aux espaces partagés actifs — jointure sur les workspaces, donc en intermediate, pas ici."
    )
}}

with source_data as (
    select * from {{ source('powerbi_activity', 'powerbi_reports') }}
),

cleaned_data as (
    select
        -- primary key
        id as report_id,

        -- foreign keys
        workspace_id,
        dataset_id,

        -- attributs (`name` préfixé : nom générique co-joint en aval avec les
        -- espaces de travail et les modèles sémantiques, cf. staging.md § 3)
        name as report_name,
        report_type,
        format,
        web_url,
        embed_url,

        -- ownership
        created_by,
        modified_by,

        -- traçabilité du filtre n°1 : null sur 100 % des lignes PAR CONSTRUCTION
        -- (`app_id is null` en clause where). Conservés pour que la question
        -- « pourquoi app_id est-il toujours vide ? » trouve sa réponse ici.
        app_id,
        original_report_object_id,

        -- dlt lineage
        _dlt_id,

        -- system columns
        timestamp(created_date_time) as created_at,
        timestamp(coalesce(modified_date_time, created_date_time)) as updated_at,
        timestamp(_extracted_at) as extracted_at,
        cast(null as timestamp) as deleted_at

    from source_data
    -- ⚠️ FILTRES OBLIGATOIRES n°1 et n°2 (cf. docs/architecture/powerbi_activity.md)
    -- BigQuery n'a pas ILIKE → lower() + not like.
    -- coalesce défensif : `not like` sur un name null renverrait null et
    -- écarterait silencieusement la ligne.
    where
        app_id is null
        and coalesce(lower(name), '') not like '%usage metrics report%'
)

select * from cleaned_data
