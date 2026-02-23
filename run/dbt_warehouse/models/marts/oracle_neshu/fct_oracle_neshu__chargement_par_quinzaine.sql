
  
    

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
        comp.name as company_name,
        EXTRACT(year from cm.task_start_date) as annee_chgt,
        FLOOR(
            DATE_DIFF(
                DATE(cm.task_start_date),
                DATE_TRUNC(
                    DATE_TRUNC(DATE(cm.task_start_date), year),
                    week (monday)
                ),
                day
            ) / 14
        ) + 1 as quinzaine_chgt,
        cm.load_quantity
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__chargement_tasks` as cm
    left join `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__product` as p
        on cm.product_id = p.product_id
    inner join `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__device` as d
        on
            cm.device_id = d.device_id
            and d.device_economic_model = 'Gratuit'
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__company` as comp
        on cm.company_id = comp.idcompany
    where cm.task_start_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), interval 730 day)
)

select
    product_type,
    company_code,
    company_name,
    annee_chgt,
    quinzaine_chgt,
    SUM(load_quantity) as quantite_chargee,
    -- Métadonnées dbt
    CURRENT_TIMESTAMP() as dbt_updated_at,
    '9cfa1134-aec6-42a6-bc9b-3a21b04af5b5' as dbt_invocation_id  -- noqa: TMP
from base
group by
    product_type,
    company_code,
    company_name,
    annee_chgt,
    quinzaine_chgt
    );
  