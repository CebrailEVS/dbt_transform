
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_neshu__chargement_quinzaine`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Quantit\u00e9s charg\u00e9es sur les machines GRATUITES uniquement, par client, type de produit, ann\u00e9e et quinzaine.\n[COMMENT CONSTRUITE] Issu de int_oracle_neshu__chargement_tasks : company_code provient directement de chargement_tasks, product_type de dim_neshu__product, company_name de dim_neshu__company ; le join dim_neshu__device sert UNIQUEMENT \u00e0 filtrer device_economic_model = 'Gratuit' (machines gratuites). Regroupement BOISSONS FRAICHES + SNACKING \u2192 SODA + SNACKS pour le reporting BI. Quinzaine = floor(jours \u00e9coul\u00e9s depuis le lundi de la semaine du 1er janvier / 14) + 1.\n[GRAIN] 1 ligne par (product_type, company_code, annee_chgt, quinzaine_chgt).\n[NOTES]\n\u26a0\ufe0f P\u00e9rim\u00e8tre restreint aux machines GRATUITES (device_economic_model = 'Gratuit') \u2014 non comparable au chargement total.\n\u26a0\ufe0f Fen\u00eatre glissante : seuls les 730 derniers jours (interval 730 day) sont inclus.\nquantite_chargee = somme de load_quantity (d\u00e9j\u00e0 d\u00e9conditionn\u00e9e par les coefficients Oracle unit_coeff_multi/div dans l'intermediate), MAIS sans le multiplicateur de conditionnement seed (units_per_pack) que fct_neshu__consommation applique en plus \u2014 d'o\u00f9 un \u00e9cart avec le chargement de fct_neshu__consommation.\nChargement filtr\u00e9 sur task_status_code in ('FAIT', 'VALIDE') (ANNULE/ANOMALIE exclus), align\u00e9 sur fct_neshu__consommation.\nquinzaine_chgt \u2208 [1, 27].\n"""
    )
    as (
      

with base as (
    select
        (case
            when p.product_type in ('BOISSONS FRAICHES', 'SNACKING') then 'SODA + SNACKS'
            else p.product_type
        end) as product_type,
        cm.company_code,
        comp.company_name,
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
    left join `evs-datastack-prod`.`prod_marts`.`dim_neshu__product` as p
        on cm.product_id = p.product_id
    inner join `evs-datastack-prod`.`prod_marts`.`dim_neshu__device` as d
        on
            cm.device_id = d.device_id
            and d.device_economic_model = 'Gratuit'
    left join `evs-datastack-prod`.`prod_marts`.`dim_neshu__company` as comp
        on cm.company_id = comp.company_id
    where
        cm.task_start_date >= timestamp_sub(current_timestamp(), interval 730 day)
        and cm.task_status_code in ('FAIT', 'VALIDE')
)

select
    product_type,
    company_code,
    company_name,
    annee_chgt,
    quinzaine_chgt,
    sum(load_quantity) as quantite_chargee,

    -- Métadonnées dbt
    current_timestamp() as dbt_updated_at,
    'fb8403f4-952c-483c-a717-204c1e040bab' as dbt_invocation_id  -- noqa: TMP
from base
group by
    product_type,
    company_code,
    company_name,
    annee_chgt,
    quinzaine_chgt
    );
  