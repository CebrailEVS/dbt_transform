
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__neshu_produit`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] R\u00e9f\u00e9rentiel des articles Neshu, tel que lu par l'application Cockpit Supply.\n[COMMENT CONSTRUITE] dim_neshu__product r\u00e9duite aux colonnes r\u00e9ellement utilis\u00e9es par l'app (relev\u00e9 du code, aucune utilisation de la table enti\u00e8re). Toutes les lignes.\n[GRAIN] 1 ligne par product_id.\n[NOTES] lue via get_dim_products, get_produits_actifs ; 37 route(s) API ; \u00e9crans : _conformite, _flux, _pilotage, _planappro, accueil, groupe, lcdp_distrib, neshu_conformite, neshu_controles, neshu_partenaires, neshu_pilotage, neshu_prevision, neshu_stocks, neshu_terrain, nunshen_partenaires, supply, technique_partenaires, v2_nav, v2_system (relev\u00e9 du 2026-10-08, tools/audit/carte_donnees.py de l'app). Les r\u00e8gles de l'app (statuts, p\u00e9riodes, exclusions) restent appliqu\u00e9es par l'app ; les filtres repris ici sont identiques et sans perte. R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

-- Référentiel des articles Neshu, pour Cockpit Supply : seules les colonnes lues par l'app.
select
    product_id,
    product_code,
    product_name,
    product_exploit,
    product_family,
    product_group,
    product_type,
    purchase_unit_price,
    product_classabc,
    product_planoete,
    product_planohiver,
    product_hpalme,
    product_bio,
    is_active
from `evs-datastack-prod`.`prod_marts`.`dim_neshu__product`
    );
  