
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__neshu_machine`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] R\u00e9f\u00e9rentiel des machines Neshu (actives ou non, pour les pr\u00e9ventives TechCare), tel que lu par l'application Cockpit Supply.\n[COMMENT CONSTRUITE] dim_neshu__device r\u00e9duite aux colonnes r\u00e9ellement utilis\u00e9es par l'app (relev\u00e9 du code, aucune utilisation de la table enti\u00e8re). Toutes les lignes.\n[GRAIN] 1 ligne par device_id.\n[NOTES] lue via get_dim_devices_neshu ; 2 route(s) API ; \u00e9crans : technique_terrain (relev\u00e9 du 2026-10-08, tools/audit/carte_donnees.py de l'app). Les r\u00e8gles de l'app (statuts, p\u00e9riodes, exclusions) restent appliqu\u00e9es par l'app ; les filtres repris ici sont identiques et sans perte. R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

-- Référentiel des machines Neshu (actives ou non, pour les préventives TechCare), pour Cockpit Supply : seules les colonnes lues par l'app.
select
    device_id,
    device_code,
    is_active,
    company_name
from `evs-datastack-prod`.`prod_marts`.`dim_neshu__device`
    );
  