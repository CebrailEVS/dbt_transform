{{
    config(
        materialized='table',
        description="Exécutions du job dbt-runner, une ligne par exécution, depuis les enveloppes de publish_run.py"
    )
}}

-- Table : la source externe est relue en entier à chaque requête ; la matérialiser ici fait que
-- l'aval et les tests lisent du stockage BigQuery au lieu de relire tous les fichiers JSON.

with source_data as (

    select
        context,
        run_results,
        sources,
        _file_name as gcs_uri
    from {{ source('dbt_artifacts', 'ext_gcs_dbt_artifacts__runner_execution') }}

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

select * from cleaned_data
