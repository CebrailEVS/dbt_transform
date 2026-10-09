
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__neshu_appro_passage`
      
    partition by timestamp_trunc(task_start_date, month)
    cluster by roadman_code, company_name

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Passages des approvisionneurs Neshu chez les clients (une t\u00e2che d'approvisionnement valid\u00e9e). Alimente dans Cockpit Supply les heures de travail des approvisionneurs (premier et dernier passage du jour) et l'onglet Clients (passages chez un client sur une p\u00e9riode).\n[COMMENT CONSTRUITE] int_oracle_neshu__appro_tasks_enriched filtr\u00e9e sur les statuts FAIT et VALIDE (config.STATUS_VALIDES de l'app), avec approvisionneur et date renseign\u00e9s. Colonnes reprises sans transformation.\n[GRAIN] 1 ligne par task_id (passage).\n[NOTES] task_start_date et task_end_date sont en heure locale fran\u00e7aise dans la source malgr\u00e9 l'\u00e9tiquette UTC du type TIMESTAMP : l'app les lit comme des heures locales sans conversion (comportement historique, cf. api/neshu.py). Les bornes propres \u00e0 l'app (bornes de dates, pas de passage futur) sont appliqu\u00e9es par ses requ\u00eates. Partition mensuelle sur task_start_date. R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

-- Passages des approvisionneurs Neshu validés (heures de travail, onglet Clients) :
-- l'app interroge cette table à la demande au lieu de garder les passages en
-- mémoire. task_start_date est en heure locale française dans la source (étiquette
-- UTC sans conversion) : repris tel quel, l'app le lit comme une heure locale.
select
    task_id,
    task_start_date,
    task_end_date,
    roadman_code,
    company_id,
    company_code,
    company_name,
    company_info,
    device_id,
    device_code,
    device_name,
    device_info,
    gea_code,
    task_location_info,
    task_status_code,
    is_done,
    is_planned,
    is_anomaly,
    passage_duration_min,
    passage_duration_hours
from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__appro_tasks_enriched`
-- Statuts retenus par l'app (config.STATUS_VALIDES) ; un passage sans
-- approvisionneur ou sans date n'est jamais affiché
where
    task_status_code in ('FAIT', 'VALIDE')
    and roadman_code is not null
    and task_start_date is not null
    );
  