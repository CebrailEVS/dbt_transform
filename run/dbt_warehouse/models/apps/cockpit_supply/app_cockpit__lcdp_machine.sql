
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__lcdp_machine`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] R\u00e9f\u00e9rentiel des machines Caf\u00e9s du Phare (comptage par approvisionneur et cat\u00e9gorie), tel que lu par l'application Cockpit Supply.\n[COMMENT CONSTRUITE] dim_lcdp__device r\u00e9duite aux colonnes r\u00e9ellement utilis\u00e9es par l'app (relev\u00e9 du code, aucune utilisation de la table enti\u00e8re). Toutes les lignes.\n[GRAIN] 1 ligne par device_id.\n[NOTES] lue via get_dim_devices_lcdp ; \u00e9crans : lcdp_pilotage. Les r\u00e8gles de l'app (statuts, p\u00e9riodes, exclusions) restent appliqu\u00e9es par l'app ; les filtres repris ici sont identiques et sans perte. R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

-- Référentiel des machines Cafés du Phare (comptage par approvisionneur et catégorie), pour Cockpit Supply : seules les colonnes lues par l'app.
select
    device_id,
    is_active,
    assigned_roadman_code,
    device_category
from `evs-datastack-prod`.`prod_marts`.`dim_lcdp__device`
    );
  