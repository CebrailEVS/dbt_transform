
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__neshu_valorisation_parc_machine`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Valorisation du parc de machines Neshu, tel que lu par l'application Cockpit Supply.\n[COMMENT CONSTRUITE] int_oracle_neshu__valorisation_parc_machines r\u00e9duite aux colonnes r\u00e9ellement utilis\u00e9es par l'app (relev\u00e9 du code, aucune utilisation de la table enti\u00e8re). Toutes les lignes.\n[GRAIN] 1 ligne par (device_name, device_group). ~27 lignes (9 groupes), couvrant ~970 machines actives pour ~101 k\u20ac de stock th\u00e9orique total.\n[NOTES] lue via get_parc_machines ; 3 route(s) API ; \u00e9crans : neshu_pilotage, neshu_stocks (relev\u00e9 du 2026-10-08, tools/audit/carte_donnees.py de l'app). Les r\u00e8gles de l'app (statuts, p\u00e9riodes, exclusions) restent appliqu\u00e9es par l'app ; les filtres repris ici sont identiques et sans perte. R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

-- Valorisation du parc de machines Neshu, pour Cockpit Supply : seules les colonnes lues par l'app.
select
    device_group,
    device_name,
    nombre_machines,
    valorisation_totale_machine
from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__valorisation_parc_machines`
    );
  