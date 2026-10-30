

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

select * from final