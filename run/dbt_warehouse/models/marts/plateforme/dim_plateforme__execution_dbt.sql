
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`dim_plateforme__execution_dbt`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Une ex\u00e9cution du runner dbt de prod : quelle s\u00e9lection, quand, combien de temps, et si elle a r\u00e9ussi. Sert \u00e0 suivre d'un coup d'\u0153il les runs de la nuit et de la journ\u00e9e.\n[COMMENT CONSTRUITE] Reprend stg_dbt_artifacts__runner_execution. selection_name est le nom court de la s\u00e9lection (source:yuman_api+ \u2192 yuman_api). Heures en heure de Paris, pour l'affichage.\n[GRAIN] 1 ligne = 1 ex\u00e9cution Cloud Run du job dbt-runner. PK : execution_id.\n[NOTES] Une ligne absente ne veut pas dire succ\u00e8s : un run tu\u00e9 (timeout, m\u00e9moire) n'\u00e9crit rien. Les builds du job cd (GitHub Actions) n'y figurent pas : seul le runner Cloud Run historise.\n"""
    )
    as (
      

with executions as (
    select * from `evs-datastack-prod`.`prod_staging`.`stg_dbt_artifacts__runner_execution`
),

final as (
    select
        date(finished_at, 'Europe/Paris') as execution_date,
        execution_name as execution_id,
        -- source:yuman_api+ → yuman_api ; selector:<nom> → <nom> ; snapshots
        regexp_extract(selection, r'^(?:source:|selector:)?([^+]+)') as selection_name,
        selection,
        command,
        exit_code,
        timestamp(datetime(started_at, 'Europe/Paris')) as started_at,
        timestamp(datetime(finished_at, 'Europe/Paris')) as finished_at,
        exit_code = 0 as is_success,
        freshness_exit_code is not null as has_freshness_check,
        coalesce(freshness_exit_code, 0) != 0 as has_freshness_error,
        full_refresh as is_full_refresh,
        round(timestamp_diff(finished_at, started_at, second) / 60, 1) as duree_min
    from executions
)

select * from final
    );
  