

  create or replace view `evs-datastack-prod`.`prod_marts`.`fct_plateforme__resultat_dbt`
  OPTIONS(
      description="""[QUOI M\u00c9TIER] Le r\u00e9sultat de chaque mod\u00e8le et de chaque test \u00e0 chaque run dbt de prod : r\u00e9ussi, en warn, en \u00e9chec ou saut\u00e9, avec le nombre de lignes en \u00e9chec. R\u00e9pond \u00e0 \u00ab qu'est-ce qui a warn\u00e9 cette nuit ? \u00bb et \u00ab depuis quand ce test warne-t-il ? \u00bb.\n[COMMENT CONSTRUITE] Reprend int_dbt_artifacts__node_result.\n[GRAIN] 1 ligne = 1 n\u0153ud (mod\u00e8le, test, seed, snapshot) dans 1 ex\u00e9cution. Cl\u00e9 : execution_id + node_id.\n[NOTES] Un run ne contient que sa s\u00e9lection : un n\u0153ud absent n'a pas tourn\u00e9. Les mod\u00e8les ephemeral, que dbt v2 note \u00ab skipped \u00bb sans les ex\u00e9cuter, sont exclus : skipped signale un vrai saut. Les nombres de warns ou d'\u00e9checs par run se comptent dans Power BI (lignes par status).\n"""
    )
  as 

with node_results as (
    select * from `evs-datastack-prod`.`prod_intermediate`.`int_dbt_artifacts__node_result`
),

final as (
    select
        date(finished_at, 'Europe/Paris') as execution_date,
        execution_name as execution_id,
        unique_id as node_id,
        node_name,
        resource_type,
        status,
        message,
        failures as nb_lignes_en_echec,
        execution_time as duree_s,
        bigquery_job_id
    from node_results
    -- Un modèle ephemeral n'est jamais exécuté (dbt l'intègre au SQL de ses enfants) : dbt v2 le
    -- note « skipped », sans relation. Exclu pour que « skipped » signale un vrai saut.
    where not (resource_type = 'model' and status = 'skipped' and relation_name is null)
)

select * from final;

