
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__lcdp_produit`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] R\u00e9f\u00e9rentiel des articles Caf\u00e9s du Phare, tel que lu par l'application Cockpit Supply.\n[COMMENT CONSTRUITE] dim_lcdp__product r\u00e9duite aux colonnes r\u00e9ellement utilis\u00e9es par l'app (relev\u00e9 du code, aucune utilisation de la table enti\u00e8re). Toutes les lignes.\n[GRAIN] 1 ligne par product_id.\n[NOTES] lue via get_dim_products_lcdp, get_produits_actifs_lcdp ; 33 route(s) API ; \u00e9crans : _flux, _pilotage, accueil, lcdp_distrib, lcdp_pilotage, lcdp_torrefaction, neshu_partenaires, nunshen_partenaires, supply, technique_partenaires (relev\u00e9 du 2026-10-08, tools/audit/carte_donnees.py de l'app). Les r\u00e8gles de l'app (statuts, p\u00e9riodes, exclusions) restent appliqu\u00e9es par l'app ; les filtres repris ici sont identiques et sans perte. R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

-- Référentiel des articles Cafés du Phare, pour Cockpit Supply : seules les colonnes lues par l'app.
select
    product_id,
    product_code,
    product_name,
    product_family,
    product_group,
    product_bio,
    is_active
from `evs-datastack-prod`.`prod_marts`.`dim_lcdp__product`
    );
  