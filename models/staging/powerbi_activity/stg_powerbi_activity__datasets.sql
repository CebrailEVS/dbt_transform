{{
    config(
        materialized='table',
        cluster_by=['workspace_id'],
        description="Modèles sémantiques Power BI nettoyés — 1 ligne = 1 modèle du locataire, PK dataset_id. Source : powerbi_datasets. Aucun filtre obligatoire sur cette table ; en revanche 8 lignes portent content_provider_type = 'UsageMetricsUserReport' (modèles des rapports de métriques auto-générés) et doivent être écartées de tout décompte de parc en aval."
    )
}}

with source_data as (
    select * from {{ source('powerbi_activity', 'powerbi_datasets') }}
),

cleaned_data as (
    select
        -- primary key
        id as dataset_id,

        -- foreign key
        workspace_id,

        -- attributs (`name` préfixé : nom générique co-joint en aval avec les
        -- espaces de travail et les rapports, cf. staging.md § 3)
        name as dataset_name,
        target_storage_mode,
        content_provider_type,
        web_url,
        create_report_embed_url,
        qna_embed_url,

        -- ownership
        configured_by,

        -- capabilities
        is_refreshable,
        add_rows_api_enabled,
        is_effective_identity_required,
        is_effective_identity_roles_required,
        is_in_place_sharing_enabled,
        query_scale_out_settings__auto_sync_read_only_replicas,
        cast(query_scale_out_settings__max_read_only_replicas as int64)
            as query_scale_out_settings__max_read_only_replicas,

        -- dlt lineage
        _dlt_id,

        -- system columns
        -- La source n'expose pas de date de modification → updated_at reprend
        -- created_at (cf. staging.md § 3 : coalesce(modif, création)).
        timestamp(created_date) as created_at,
        timestamp(created_date) as updated_at,
        timestamp(_extracted_at) as extracted_at,
        cast(null as timestamp) as deleted_at

    from source_data
)

select * from cleaned_data
