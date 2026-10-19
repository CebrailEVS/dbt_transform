
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__lcdp_machine`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] R\u00e9f\u00e9rentiel des machines Caf\u00e9s du Phare, tel que lu par l'application Cockpit Supply : comptage par approvisionneur et cat\u00e9gorie, et \u00e9cran \u00c9quipements en stock (machines en d\u00e9p\u00f4t, avec n\u00b0 de s\u00e9rie, propri\u00e9taire et achat).\n[COMMENT CONSTRUITE] dim_lcdp__device r\u00e9duite aux colonnes utilis\u00e9es par ces deux \u00e9crans. Toutes les lignes, sans filtre de d\u00e9p\u00f4t ni de statut.\n[GRAIN] 1 ligne par device_id.\n[NOTES] lue via get_dim_devices_lcdp ; \u00e9crans : lcdp_pilotage, \u00e9quipements en stock. Le d\u00e9p\u00f4t d'une machine en stock est company_code / company_name (DEPOT ATELIER, DEPOT NEUF, DEPOT CASSE, DEPOT BOUTIQUE). Les labels sont des libell\u00e9s (device_material_status \u00ab EN STOCK \u00bb), contrairement \u00e0 Neshu qui expose des codes. Pas de champ de r\u00e9servation : la famille n'existe pas dans l'ERP LCDP. Les r\u00e8gles de l'app (d\u00e9p\u00f4ts retenus, statuts, exclusions) restent appliqu\u00e9es par l'app. R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

-- Référentiel des machines Cafés du Phare pour Cockpit Supply : comptage par approvisionneur et catégorie, et
-- écran Équipements en stock (dépôt, n° de série, propriétaire, achat). Toutes les machines, sans filtre.
select
    device_id,
    device_code,
    device_name,
    device_serial_number,
    company_code,
    company_name,
    owner_company_name,
    device_brand,
    device_category,
    device_state,
    device_material_status,
    device_comment,
    assigned_roadman_code,
    is_active,
    purchase_date,
    purchase_cost
from `evs-datastack-prod`.`prod_marts`.`dim_lcdp__device`
    );
  