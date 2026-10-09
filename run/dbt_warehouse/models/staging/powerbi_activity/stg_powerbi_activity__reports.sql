
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_powerbi_activity__reports`
      
    
    cluster by workspace_id, dataset_id

    
    OPTIONS(
      description="""Grain : 1 ligne = 1 rapport original, hors rapports de m\u00e9triques d'usage. Deux filtres obligatoires appliqu\u00e9s.\n\u26a0\ufe0f CE MOD\u00c8LE N'EST PAS LE P\u00c9RIM\u00c8TRE M\u00c9TIER. Les \u00ab rapports m\u00e9tier \u00bb exigent EN PLUS la restriction aux espaces partag\u00e9s actifs \u2014 soit une jointure vers les workspaces, interdite en staging (staging.md \u00a7 1 et \u00a7 9 : une source de staging = une table source, pas de jointure, pas de logique de p\u00e9rim\u00e8tre). Cette restriction appartient \u00e0 l'intermediate. Le p\u00e9rim\u00e8tre m\u00e9tier s'obtient en trois restrictions successives : espace partag\u00e9 actif, hors copies d'App, hors m\u00e9triques d'usage.\n"""
    )
    as (
      

with source_data as (
    select * from `evs-datastack-prod`.`prod_raw`.`powerbi_reports`
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

        -- traçabilité du filtre n°1 : toujours null PAR CONSTRUCTION
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
    );
  