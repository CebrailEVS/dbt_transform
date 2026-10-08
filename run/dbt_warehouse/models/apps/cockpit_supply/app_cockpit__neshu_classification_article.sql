
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__neshu_classification_article`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Classification de la demande des articles Neshu (ADI / CV\u00b2, saisonnalit\u00e9), tel que lu par l'application Cockpit Supply.\n[COMMENT CONSTRUITE] fct_supply_chain__classification_article_neshu r\u00e9duite aux colonnes r\u00e9ellement utilis\u00e9es par l'app (relev\u00e9 du code, aucune utilisation de la table enti\u00e8re). Toutes les lignes.\n[GRAIN] 1 ligne par (company_id, product_id) \u2014 \u00e9tat courant recalcul\u00e9 \u00e0 chaque build.\n[NOTES] lue via get_point_commande_neshu ; 16 route(s) API ; \u00e9crans : _conformite, _pilotage, _planappro, accueil, groupe, neshu_conformite, neshu_controles, neshu_partenaires, neshu_pilotage, neshu_prevision, neshu_stocks, neshu_terrain, supply, v2_nav, v2_system (relev\u00e9 du 2026-10-08, tools/audit/carte_donnees.py de l'app). Les r\u00e8gles de l'app (statuts, p\u00e9riodes, exclusions) restent appliqu\u00e9es par l'app ; les filtres repris ici sont identiques et sans perte. R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

-- Classification de la demande des articles Neshu (ADI / CV², saisonnalité), pour Cockpit Supply : seules les colonnes lues par l'app.
select
    company_id,
    product_id,
    classe_demande,
    alerte_exploit,
    alerte_saison,
    saison_produit,
    en_saison,
    adi,
    cv2,
    valeur_12m
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__classification_article_neshu`
    );
  