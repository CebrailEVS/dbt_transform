
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_powerbi_activity__datasets`
      
    
    cluster by workspace_id

    
    OPTIONS(
      description="""Grain : 1 ligne = 1 mod\u00e8le s\u00e9mantique du locataire. 125 lignes au 2026-09-03, aucun filtre obligatoire.\n\u00c0 savoir en aval : 8 lignes portent `content_provider_type = 'UsageMetricsUserReport'` \u2014 ce sont les mod\u00e8les des rapports de m\u00e9triques d'usage auto-g\u00e9n\u00e9r\u00e9s par Power BI, pendant du filtre n\u00b02 c\u00f4t\u00e9 rapports. \u00c0 \u00e9carter de tout d\u00e9compte de parc ou de co\u00fbt de rafra\u00eechissement.\n"""
    )
    as (
      

with source_data as (
    select * from `evs-datastack-prod`.`prod_raw`.`powerbi_datasets`
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
    );
  