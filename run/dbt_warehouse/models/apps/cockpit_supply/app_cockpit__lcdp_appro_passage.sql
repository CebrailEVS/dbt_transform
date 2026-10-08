
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__lcdp_appro_passage`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Passages des approvisionneurs Caf\u00e9s du Phare Distribution Auto, tel que lu par l'application Cockpit Supply.\n[COMMENT CONSTRUITE] int_oracle_lcdp__appro_tasks_enriched r\u00e9duite aux colonnes r\u00e9ellement utilis\u00e9es par l'app (relev\u00e9 du code, aucune utilisation de la table enti\u00e8re). Lignes : passages valid\u00e9s (FAIT, VALIDE) avec approvisionneur, depuis le 2026-01-01 (bornes de l'app).\n[GRAIN] 1 ligne par task_id (PK), uniquement les passages avec roadman.\n[NOTES] lue via get_appro_tasks_enriched_lcdp ; 1 route(s) API ; \u00e9crans : lcdp_distrib (relev\u00e9 du 2026-10-08, tools/audit/carte_donnees.py de l'app). Les r\u00e8gles de l'app (statuts, p\u00e9riodes, exclusions) restent appliqu\u00e9es par l'app ; les filtres repris ici sont identiques et sans perte. R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

-- Passages des approvisionneurs Cafés du Phare Distribution Auto, pour Cockpit Supply : seules les colonnes lues par l'app,
-- passages validés (FAIT, VALIDE) avec approvisionneur, depuis le 2026-01-01 (bornes de l'app).
select
    task_id,
    roadman_code,
    task_start_date,
    task_status_code
from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__appro_tasks_enriched`
where task_status_code in ('FAIT', 'VALIDE') and roadman_code is not null and task_start_date is not null and task_start_date >= timestamp('2026-01-01')
    );
  