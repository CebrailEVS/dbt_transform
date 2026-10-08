
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__technique_intervention`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Interventions des techniciens TechCare utiles \u00e0 Cockpit Supply : r\u00e9alis\u00e9es (onglet Clients, derni\u00e8res pr\u00e9ventives) et pr\u00e9ventives planifi\u00e9es.\n[COMMENT CONSTRUITE] int_yuman__interventions filtr\u00e9e (REALISEE avec date de r\u00e9alisation, ou pr\u00e9ventive PLANIFIEE), 10 colonnes sur 58 (sans les textes libres).\n[GRAIN] 1 ligne par couple (demand_id, workorder_id) de la source, restreinte aux interventions ci-dessus.\n[NOTES] Les r\u00e8gles de l'app (machine actives, clients g\u00e9n\u00e9riques exclus, statut de pr\u00e9ventive selon la date du jour) restent dans l'app. R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

-- Interventions TechCare utiles à l'app : réalisées (onglet Clients, préventives
-- réalisées) et préventives planifiées ; 10 colonnes sur 58 (sans le texte libre).
select
    material_id,
    material_serial_number,
    workorder_category,
    client_name,
    site_name,
    machine_clean,
    workorder_type_grouped,
    intervention_state,
    date_done,
    date_planned
from `evs-datastack-prod`.`prod_intermediate`.`int_yuman__interventions`
where
    (intervention_state = 'REALISEE' and date_done is not null)
    or (workorder_type_grouped = 'Preventive' and intervention_state = 'PLANIFIEE')
    );
  