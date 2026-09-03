{{
    config(
        materialized='table',
        partition_by={'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day'},
        cluster_by=['activity', 'report_id', 'workspace_id', 'dataset_id'],
        description="Événements d'audit Power BI nettoyés — 1 ligne = 1 événement du journal d'audit unifié, PK event_id. Source : powerbi_activity_events. AUCUN filtre d'activité ici : RefreshDataset (~83 % du volume) est conservé volontairement, il sert de monitoring des pipelines et de base au coût de maintien des rapports dormants. Filtrer activity = 'ViewReport' en aval pour mesurer l'usage humain. Partitionné sur created_at pour l'élagage de partition."
    )
}}

with source_data as (
    select * from {{ source('powerbi_activity', 'powerbi_activity_events') }}
),

cleaned_data as (
    select
        -- primary key
        id as event_id,

        -- event classification
        cast(record_type as int64) as record_type,
        operation,
        activity,
        workload,
        is_success,

        -- tenant / request context
        organization_id,
        request_id,
        activity_id,

        -- user (DONNÉE PERSONNELLE — agréger en aval, ne jamais exposer nominativement)
        user_id,
        user_key,
        cast(user_type as int64) as user_type,
        client_ip,
        user_agent,

        -- generic object targeted by the event
        object_id,
        item_id,
        item_name,
        artifact_id,
        artifact_name,
        artifact_kind,

        -- report context (renseigné sur les seuls événements de consultation)
        report_id,
        report_name,
        report_type,

        -- workspace context
        -- `work_space_name` : l'API rend `WorkSpaceName` face à `WorkspaceId`.
        -- Incohérence de casse à la source, pas une faute du pipeline.
        workspace_id,
        work_space_name,

        -- dataset context
        dataset_id,
        dataset_name,

        -- capacity context
        capacity_id,
        capacity_name,

        -- app context (consultation via une App publiée)
        app_id,
        app_name,
        app_report_id,

        -- consumption context
        distribution_method,
        consumption_method,

        -- refresh context (activity = 'RefreshDataset')
        refresh_type,
        timestamp(last_refresh_time) as last_refresh_time,
        data_connectivity_mode,
        cast(refresh_enforcement_policy as int64) as refresh_enforcement_policy,
        cast(billing_type as int64) as billing_type,
        schedules__refresh_frequency,
        schedules__time_zone,

        -- export context
        exported_artifact_info__export_type,
        exported_artifact_info__artifact_type,
        cast(exported_artifact_info__artifact_id as int64) as exported_artifact_info__artifact_id,
        exported_artifact_download_info__export_type,
        exported_artifact_download_info__artifact_object_id,
        exported_artifact_download_info__artifact_name,
        exported_artifact_download_info__export_id,
        -- STRING à la source → safe_cast (cf. staging.md § 4)
        safe_cast(exported_artifact_download_info__export_creation_time as timestamp)
            as exported_artifact_download_info__export_creation_time,
        cast(exported_artifact_download_info__page_count as int64) as exported_artifact_download_info__page_count,

        -- dlt lineage : clé parente des tables enfants __schedules__* / __models_snapshots
        _dlt_id,

        -- system columns
        -- created_at = horodatage de l'événement ; un événement d'audit est
        -- immuable, donc updated_at le reprend. La source ne porte aucune
        -- notion de suppression → deleted_at null.
        timestamp(creation_time) as created_at,
        timestamp(creation_time) as updated_at,
        timestamp(_extracted_at) as extracted_at,
        cast(null as timestamp) as deleted_at

    from source_data
)

select * from cleaned_data
