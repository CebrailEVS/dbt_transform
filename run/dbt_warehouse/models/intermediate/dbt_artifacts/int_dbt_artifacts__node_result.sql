

  create or replace view `evs-datastack-prod`.`prod_intermediate`.`int_dbt_artifacts__node_result`
  OPTIONS(
      description="""[QUOI M\u00c9TIER] Le r\u00e9sultat de chaque mod\u00e8le, test, seed ou snapshot \u00e0 chaque ex\u00e9cution du runner dbt de prod : statut, lignes en \u00e9chec, dur\u00e9e, volume lu. Base du suivi de sant\u00e9 de l'entrep\u00f4t : warns chroniques, \u00e9checs, n\u0153uds saut\u00e9s, d\u00e9rive des dur\u00e9es.\n[COMMENT CONSTRUITE] D\u00e9pliage de la liste node_results de stg_dbt_artifacts__runner_execution. Le type et le nom du n\u0153ud sont tir\u00e9s de son unique_id.\n[GRAIN] 1 ligne = 1 n\u0153ud dans 1 ex\u00e9cution. Cl\u00e9 : execution_name + unique_id.\n[NOTES] Une ex\u00e9cution ne contient que sa s\u00e9lection : un n\u0153ud absent n'a pas tourn\u00e9, il n'a pas \u00ab r\u00e9ussi \u00bb. Vue, comme la staging : toujours \u00e0 jour, tests au d\u00e9ploiement seulement.\n"""
    )
  as 

with executions as (
    select * from `evs-datastack-prod`.`prod_staging`.`stg_dbt_artifacts__runner_execution`
),

node_results as (
    select
        e.execution_name,
        e.selection,
        e.finished_at,
        node.unique_id,
        split(node.unique_id, '.')[safe_offset(0)] as resource_type,
        -- unit_test.<projet>.<modèle>.<test> : le nom du test est au 4e rang, pas au 3e.
        split(node.unique_id, '.')[safe_offset(if(node.unique_id like 'unit_test.%', 3, 2))] as node_name,
        node.status,
        node.failures,
        node.message,
        node.execution_time,
        node.relation_name,
        node.bigquery_job_id
    from executions as e
    cross join unnest(e.node_results) as node
)

select * from node_results;

