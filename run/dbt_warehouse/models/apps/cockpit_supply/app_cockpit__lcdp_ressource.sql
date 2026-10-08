
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__lcdp_ressource`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] R\u00e9f\u00e9rentiel des ressources Caf\u00e9s du Phare (v\u00e9hicules), tel que lu par l'application Cockpit Supply.\n[COMMENT CONSTRUITE] dim_lcdp__resource r\u00e9duite aux colonnes r\u00e9ellement utilis\u00e9es par l'app (relev\u00e9 du code, aucune utilisation de la table enti\u00e8re). Toutes les lignes.\n[GRAIN] 1 ligne par resources_id.\n[NOTES] lue via get_dim_resources_lcdp ; 13 route(s) API ; \u00e9crans : _flux, lcdp_distrib, lcdp_torrefaction, supply (relev\u00e9 du 2026-10-08, tools/audit/carte_donnees.py de l'app). Les r\u00e8gles de l'app (statuts, p\u00e9riodes, exclusions) restent appliqu\u00e9es par l'app ; les filtres repris ici sont identiques et sans perte. R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

-- Référentiel des ressources Cafés du Phare (véhicules), pour Cockpit Supply : seules les colonnes lues par l'app.
select
    resources_id,
    resources_code,
    resources_name,
    resources_type
from `evs-datastack-prod`.`prod_marts`.`dim_lcdp__resource`
    );
  