
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_powerbi_activity__events`
      
    partition by timestamp_trunc(created_at, day)
    cluster by activity, report_id, workspace_id, dataset_id

    
    OPTIONS(
      description="""Grain : 1 ligne = 1 \u00e9v\u00e9nement du journal d'audit unifi\u00e9 Power BI. Toutes les activit\u00e9s sont conserv\u00e9es (aucun filtre) \u2014 voir le config block.\nContrainte RGPD : `user_id` / `user_key` identifient nominativement un salari\u00e9. Le dispositif rel\u00e8ve du suivi d'activit\u00e9, avec une finalit\u00e9 d\u00e9clar\u00e9e de rationalisation du parc de rapports, PAS d'\u00e9valuation des personnes. Les mod\u00e8les aval n'exposent que des agr\u00e9gats (count(distinct user_id)) \u2014 jamais \u00ab qui a ouvert quel rapport \u00bb. Cf. ingestion/pipelines/powerbi_activity/HABILITATION.md.\n"""
    )
    as (
      

with source_data as (
    select * from `evs-datastack-prod`.`prod_raw`.`powerbi_activity_events`
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
    );
  