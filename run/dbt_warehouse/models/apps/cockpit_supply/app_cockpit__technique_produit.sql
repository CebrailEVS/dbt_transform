
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__technique_produit`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] R\u00e9f\u00e9rentiel des articles TechCare (pi\u00e8ces interdites / obligatoires), tel que lu par l'application Cockpit Supply.\n[COMMENT CONSTRUITE] stg_yuman__products r\u00e9duite aux colonnes lues par l'app. Toutes les lignes.\n[GRAIN] 1 ligne par `product_id` (PK).\n[NOTES] lue via _get_dim_technique_product ; \u00e9crans : _conformite, _flux, _planappro, accueil, groupe, supply, technique_partenaires, technique_pilotage, technique_stocks, technique_terrain, v2_nav. Les r\u00e8gles de l'app (statuts, p\u00e9riodes, exclusions) restent appliqu\u00e9es par l'app ; les filtres repris ici sont identiques et sans perte. R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

-- Référentiel des articles TechCare (pièces interdites / obligatoires) pour Cockpit Supply, construit depuis le staging.
select
    product_id,
    product_code,
    product_name,
    is_forbidden_article,
    is_mandatory_article
from `evs-datastack-prod`.`prod_staging`.`stg_yuman__products`
    );
  