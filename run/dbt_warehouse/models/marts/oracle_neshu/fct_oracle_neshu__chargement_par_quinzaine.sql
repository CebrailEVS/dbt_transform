
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_oracle_neshu__chargement_par_quinzaine`
      
    
    

    
    OPTIONS(
      description="""Table de faits calculant les quantit\u00e9s charg\u00e9es par type de produit, soci\u00e9t\u00e9, ann\u00e9e et quinzaine (p\u00e9riodes de 14 jours d\u00e9marrant un lundi).\n"""
    )
    as (
      -- models/fct_chargement_quinzaine.sql


with base as (
    select
        p.product_type,
        cm.company_code,
        extract(year from cm.task_start_date) as annee_chgt,
        floor(
            date_diff(
                date(cm.task_start_date),
                date_trunc(
                    date_trunc(date(cm.task_start_date), year),
                    week (monday)
                ),
                day
            ) / 14
        ) + 1 as quinzaine_chgt,
        cm.load_quantity
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__chargement_tasks` as cm
    left join `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__product` as p
        on cm.product_id = p.product_id
    where cm.task_start_date >= timestamp(
        datetime_sub(current_datetime(), interval 24 month)
    )
)

select
    product_type,
    company_code,
    annee_chgt,
    quinzaine_chgt,
    sum(load_quantity) as quantite_chargee,
    -- Métadonnées dbt
    current_timestamp() as dbt_updated_at,
    '2c49c569-b4fc-4f29-9309-0a459bd137af' as dbt_invocation_id  -- noqa: TMP
from base
group by
    product_type,
    company_code,
    annee_chgt,
    quinzaine_chgt
order by
    product_type,
    company_code,
    annee_chgt,
    quinzaine_chgt
    );
  