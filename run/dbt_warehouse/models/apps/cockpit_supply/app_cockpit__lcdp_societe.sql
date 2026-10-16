
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__lcdp_societe`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] R\u00e9f\u00e9rentiel des soci\u00e9t\u00e9s Caf\u00e9s du Phare, tel que lu par l'application Cockpit Supply.\n[COMMENT CONSTRUITE] dim_lcdp__company r\u00e9duite aux colonnes r\u00e9ellement utilis\u00e9es par l'app (relev\u00e9 du code, aucune utilisation de la table enti\u00e8re). Toutes les lignes.\n[GRAIN] 1 ligne par company_id.\n[NOTES] lue via get_livraison_lcdp, get_reception_lcdp ; \u00e9crans : _conformite, _flux, _pilotage, accueil, groupe, lcdp_distrib, lcdp_pilotage, lcdp_torrefaction, neshu_partenaires, nunshen_partenaires, supply, technique_partenaires, v2_nav. Les r\u00e8gles de l'app (statuts, p\u00e9riodes, exclusions) restent appliqu\u00e9es par l'app ; les filtres repris ici sont identiques et sans perte. R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

-- Référentiel des sociétés Cafés du Phare, pour Cockpit Supply : seules les colonnes lues par l'app.
select
    company_id,
    company_code,
    company_name
from `evs-datastack-prod`.`prod_marts`.`dim_lcdp__company`
    );
  