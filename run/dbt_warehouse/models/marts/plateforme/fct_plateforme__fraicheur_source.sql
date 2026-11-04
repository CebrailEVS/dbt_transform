
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_plateforme__fraicheur_source`
      
    partition by execution_date
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] L'\u00e2ge de chaque table source \u00e0 chaque contr\u00f4le de fra\u00eecheur fait avant un run dbt de prod, et son verdict au regard des seuils de docs/freshness.md.\n[COMMENT CONSTRUITE] Reprend int_dbt_artifacts__source_freshness. Heures en heure de Paris, pour l'affichage.\n[GRAIN] 1 ligne = 1 table source dans 1 contr\u00f4le. Cl\u00e9 : execution_id + source_name + table_name.\n[NOTES] Seuls les runs lanc\u00e9s avec un contr\u00f4le de fra\u00eecheur en produisent.\n"""
    )
    as (
      

with source_checks as (
    select * from `evs-datastack-prod`.`prod_intermediate`.`int_dbt_artifacts__source_freshness`
),

final as (
    select
        date(finished_at, 'Europe/Paris') as execution_date,
        execution_name as execution_id,
        source_name,
        table_name,
        status,
        timestamp(datetime(max_loaded_at, 'Europe/Paris')) as max_loaded_at,
        timestamp(datetime(checked_at, 'Europe/Paris')) as checked_at,
        age_hours as age_heures
    from source_checks
)

select * from final
    );
  