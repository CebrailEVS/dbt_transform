
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__neshu_chargement`
      
    partition by timestamp_trunc(task_start_date, month)
    cluster by vehicle_code, task_id

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Chargements machines Neshu valid\u00e9s (articles charg\u00e9s par les approvisionneurs dans les distributeurs), avec le client et la machine. Alimente les \u00e9crans Cockpit Supply \u00ab Contr\u00f4le des conditionnements \u00bb, \u00ab Clients \u00bb, flux des v\u00e9hicules et bilan mensuel.\n[COMMENT CONSTRUITE] int_oracle_neshu__chargement_tasks filtr\u00e9e sur les statuts FAIT et VALIDE (config.STATUS_VALIDES de l'app), jointe \u00e0 dim_neshu__company (company_name) et dim_neshu__device (device_name, device_brand) sur leurs identifiants. Colonnes reprises sans transformation.\n[GRAIN] 1 ligne par task_product_id (article d'un bordereau de chargement).\n[NOTES] L'app envoie des requ\u00eates filtr\u00e9es et ne re\u00e7oit que les r\u00e9sultats. Partition mensuelle sur task_start_date (UTC, comme les mois de l'app), clustering vehicle_code / task_id (filtre v\u00e9hicule, d\u00e9tail d'un bordereau). Champs du bordereau (date, v\u00e9hicule, approvisionneur, client, machine) constants par task_id. R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

-- Chargements machines Neshu validés, enrichis du client et de la machine : l'app
-- interroge cette table à la demande (filtres, regroupements, période) au lieu de
-- garder tout l'historique en mémoire. Partition mensuelle : un écran filtré sur
-- un mois ne lit que ce mois.
select
    c.task_product_id,
    c.task_id,
    c.task_start_date,
    c.task_status_code,
    c.load_type_code,
    c.task_location_info,
    c.vehicle_code,
    c.roadman_code,
    c.company_id,
    c.company_code,
    co.company_name,
    c.device_id,
    c.device_code,
    d.device_name,
    d.device_brand,
    c.product_code,
    c.unit_coeff_multi,
    c.unit_coeff_div,
    c.base_unit_quantity,
    c.load_quantity,
    c.load_valuation
from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__chargement_tasks` as c
left join `evs-datastack-prod`.`prod_marts`.`dim_neshu__company` as co
    on c.company_id = co.company_id
left join `evs-datastack-prod`.`prod_marts`.`dim_neshu__device` as d
    on c.device_id = d.device_id
-- Statuts retenus par l'app (config.STATUS_VALIDES)
where c.task_status_code in ('FAIT', 'VALIDE')
    );
  