
  
    

    create or replace table `evs-datastack-prod`.`prod_intermediate`.`int_dbt_artifacts__source_freshness`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] L'\u00e2ge de chaque table source \u00e0 chaque contr\u00f4le de fra\u00eecheur du runner dbt de prod, et le verdict au regard de ses seuils warn_after et error_after.\n[COMMENT CONSTRUITE] D\u00e9pliage de la liste freshness_results de stg_dbt_artifacts__runner_execution. Source et table sont tir\u00e9es de l'unique_id.\n[GRAIN] 1 ligne = 1 table source dans 1 contr\u00f4le. Cl\u00e9 : execution_name + unique_id.\n[NOTES] Seules les ex\u00e9cutions lanc\u00e9es avec un contr\u00f4le de fra\u00eecheur (DBT_SOURCE_SELECTOR) en produisent. Reconstruit avec la staging par le workflow transform-plateforme.\n"""
    )
    as (
      

with executions as (
    select * from `evs-datastack-prod`.`prod_staging`.`stg_dbt_artifacts__runner_execution`
),

source_checks as (
    select
        e.execution_name,
        e.freshness_selection,
        e.finished_at,
        source_check.unique_id,
        split(source_check.unique_id, '.')[safe_offset(2)] as source_name,
        split(source_check.unique_id, '.')[safe_offset(3)] as table_name,
        source_check.status,
        source_check.max_loaded_at,
        source_check.snapshotted_at as checked_at,
        source_check.max_loaded_at_time_ago_in_s / 3600 as age_hours
    from executions as e
    cross join unnest(e.freshness_results) as source_check
)

select * from source_checks
    );
  