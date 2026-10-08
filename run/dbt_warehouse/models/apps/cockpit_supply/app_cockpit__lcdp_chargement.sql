
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__lcdp_chargement`
      
    partition by timestamp_trunc(task_start_date, month)
    cluster by vehicle_code, task_id

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Chargements machines Caf\u00e9s du Phare Distribution Auto valid\u00e9s (articles charg\u00e9s par les approvisionneurs dans les distributeurs). Alimente le flux des v\u00e9hicules de tourn\u00e9e Caf\u00e9s du Phare dans Cockpit Supply.\n[COMMENT CONSTRUITE] int_oracle_lcdp__chargement_tasks filtr\u00e9e sur les statuts FAIT et VALIDE (config.STATUS_VALIDES de l'app). Colonnes reprises sans transformation.\n[GRAIN] 1 ligne par task_product_id (article d'un bordereau de chargement).\n[NOTES] Remplace dans l'app la lecture en m\u00e9moire de tout l'historique (~370 k lignes) : l'app lit le seul mois affich\u00e9. Partition mensuelle sur task_start_date (UTC, comme les mois de l'app). M\u00eame construction que app_cockpit__neshu_chargement, sans jointure client / machine (aucun \u00e9cran n'en a besoin). R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

-- Chargements machines Cafés du Phare Distribution Auto validés : l'app en lit un
-- mois à la fois (flux des véhicules) au lieu de garder tout l'historique en
-- mémoire. Partition mensuelle : un mois demandé = un mois lu.
select
    task_product_id,
    task_id,
    task_start_date,
    task_status_code,
    load_type_code,
    vehicle_code,
    roadman_code,
    company_id,
    company_code,
    device_id,
    device_code,
    product_code,
    unit_coeff_multi,
    unit_coeff_div,
    base_unit_quantity,
    load_quantity,
    load_valuation
from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__chargement_tasks`
-- Statuts retenus par l'app (config.STATUS_VALIDES)
where task_status_code in ('FAIT', 'VALIDE')
    );
  