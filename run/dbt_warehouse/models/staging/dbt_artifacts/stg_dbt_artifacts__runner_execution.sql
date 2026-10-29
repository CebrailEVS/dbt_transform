

  create or replace view `evs-datastack-prod`.`prod_staging`.`stg_dbt_artifacts__runner_execution`
  OPTIONS(
      description="""Grain : 1 ligne = 1 ex\u00e9cution Cloud Run du job dbt-runner (un fichier d\u00e9pos\u00e9 par publish_run.py). Les listes node_results (un \u00e9l\u00e9ment par mod\u00e8le, test, seed ou snapshot ex\u00e9cut\u00e9) et freshness_results (un \u00e9l\u00e9ment par table source contr\u00f4l\u00e9e) sont d\u00e9pli\u00e9es en intermediate.\nVue, par exception \u00e0 la convention \u00ab staging = table \u00bb : la source est \u00e9crite par le runner lui-m\u00eame, sans pipeline qui reconstruirait une table. Chaque lecture relit les fichiers : la vue est toujours \u00e0 jour. Contrepartie : ses tests ne tournent qu'au d\u00e9ploiement.\n"""
    )
  as 

-- Vue et non table : la source est écrite par le runner lui-même, une quinzaine de fois par jour,
-- sans pipeline qui reconstruirait une table ; à ~5 Mo par jour, relire les fichiers ne coûte rien.

with source_data as (

    select
        context,
        run_results,
        sources,
        _file_name as gcs_uri
    from `evs-datastack-prod`.`prod_raw`.`ext_gcs_dbt_artifacts__runner_execution`

),

cleaned_data as (

    select
        -- Renommé : `execution` est un mot-clé SQL.
        context.execution as execution_name,
        context.job,
        context.command,
        context.selection,
        context.freshness_selection,
        context.full_refresh,
        context.exit_code,
        context.freshness_exit_code,
        safe.timestamp(context.started_at) as started_at,
        safe.timestamp(context.finished_at) as finished_at,

        -- Les deux artefacts portent les mêmes champs (metadata, results) : préfixés pour cohabiter.
        coalesce(run_results.metadata.dbt_version, sources.metadata.dbt_version) as dbt_version,
        run_results.metadata.invocation_id as run_invocation_id,
        run_results.elapsed_time as run_elapsed_time,
        array(
            select as struct
                node.unique_id,
                lower(node.status) as status,
                node.failures,
                node.message,
                node.execution_time,
                node.relation_name,
                -- bytes_processed vaut toujours 0 en dbt v2 : le coût se lit dans INFORMATION_SCHEMA.JOBS.
                node.adapter_response.job_id as bigquery_job_id
            from unnest(run_results.results) as node
        ) as node_results,

        sources.metadata.invocation_id as freshness_invocation_id,
        sources.elapsed_time as freshness_elapsed_time,
        array(
            select as struct
                source_check.unique_id,
                -- dbt v2 écrit « Pass », « Warn » en casse mixte pour la fraîcheur.
                lower(source_check.status) as status,
                -- Horodatages à la nanoseconde, tronqués à la microseconde de TIMESTAMP.
                safe.timestamp(
                    regexp_replace(source_check.max_loaded_at, r'(\.\d{6})\d+', r'\1')
                ) as max_loaded_at,
                safe.timestamp(
                    regexp_replace(source_check.snapshotted_at, r'(\.\d{6})\d+', r'\1')
                ) as snapshotted_at,
                source_check.max_loaded_at_time_ago_in_s,
                source_check.execution_time
            from unnest(sources.results) as source_check
        ) as freshness_results,

        gcs_uri,
        safe.timestamp(context.finished_at) as extracted_at

    from source_data

)

select * from cleaned_data;

