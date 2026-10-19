
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__neshu_machine`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] R\u00e9f\u00e9rentiel des machines Neshu (actives ou non), tel que lu par l'application Cockpit Supply : pr\u00e9ventives TechCare et \u00e9cran \u00c9quipements en stock (machines en d\u00e9p\u00f4t ou en atelier, avec n\u00b0 de s\u00e9rie, propri\u00e9taire, achat et r\u00e9servation).\n[COMMENT CONSTRUITE] dim_neshu__device r\u00e9duite aux colonnes utilis\u00e9es par ces deux \u00e9crans. Toutes les lignes, sans filtre de d\u00e9p\u00f4t ni de statut.\n[GRAIN] 1 ligne par device_id.\n[NOTES] lue via get_dim_devices_neshu ; \u00e9crans : technique_terrain, \u00e9quipements en stock. Le d\u00e9p\u00f4t d'une machine en stock est company_code / company_name (ex. DEPOTATELIERRUNGIS / \u00ab 06 - ATELIER RUNGIS DEPOT \u00bb) ; company_code se rapproche de destination_code de app_cockpit__neshu_commande_fournisseur. Les labels sont des codes ERP (device_material_status S = en stock), contrairement \u00e0 LCDP qui expose des libell\u00e9s. Les r\u00e8gles de l'app (d\u00e9p\u00f4ts retenus, statuts, exclusions) restent appliqu\u00e9es par l'app. R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

-- Référentiel des machines Neshu pour Cockpit Supply : préventives TechCare (actives ou non) et écran
-- Équipements en stock (dépôt, n° de série, propriétaire, achat, réservation). Toutes les machines, sans filtre.
select
    device_id,
    device_code,
    device_name,
    device_serial_number,
    company_code,
    company_name,
    owner_company_name,
    device_brand,
    device_gamme,
    device_category,
    device_state,
    device_material_status,
    device_comment,
    is_active,
    is_reserved,
    purchase_date,
    purchase_cost
from `evs-datastack-prod`.`prod_marts`.`dim_neshu__device`
    );
  