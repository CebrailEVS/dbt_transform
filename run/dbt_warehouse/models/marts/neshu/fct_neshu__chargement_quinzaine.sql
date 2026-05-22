
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_neshu__chargement_quinzaine`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Quantit\u00e9s charg\u00e9es par client, type de produit, ann\u00e9e et quinzaine.\n[COMMENT CONSTRUITE] Issu de int_oracle_neshu__chargement_tasks joint \u00e0 dim_neshu__product (product_type) et dim_neshu__device (company_code). Regroupement BOISSONS FRAICHES + SNACKING \u2192 SODA + SNACKS pour le reporting BI. Quinzaine calcul\u00e9e depuis le lundi de r\u00e9f\u00e9rence de l'ann\u00e9e (FLOOR(jours / 14) + 1).\n[GRAIN] 1 ligne par (product_type, company_code, annee_chgt, quinzaine_chgt).\n[NOTES] quinzaine_chgt \u2208 [1, 27].\n"""
    )
    as (
      -- models/fct_chargement_quinzaine.sql


with base as (
    select
        (case
            when p.product_type in ('BOISSONS FRAICHES', 'SNACKING') then 'SODA + SNACKS'
            else p.product_type
        end) as product_type,
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
    left join `evs-datastack-prod`.`prod_marts`.`dim_neshu__product` as p
        on cm.product_id = p.product_id
    inner join `evs-datastack-prod`.`prod_marts`.`dim_neshu__device` as d
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
    'c4f6f006-9720-4500-a4b4-44785657db4c' as dbt_invocation_id  -- noqa: TMP
from base
group by
    product_type,
    company_code,
    company_name,
    annee_chgt,
    quinzaine_chgt
    );
  