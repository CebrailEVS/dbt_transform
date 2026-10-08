
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__neshu_point_commande`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Point de commande Neshu (\u00ab Commandes \u00e0 passer \u00bb), tel que lu par l'application Cockpit Supply.\n[COMMENT CONSTRUITE] fct_supply_chain__point_commande_neshu r\u00e9duite aux colonnes r\u00e9ellement utilis\u00e9es par l'app (relev\u00e9 du code, aucune utilisation de la table enti\u00e8re). Toutes les lignes.\n[GRAIN] 1 ligne par (company_id, product_id) \u2014 \u00e9tat courant recalcul\u00e9 \u00e0 chaque build, align\u00e9 sur la classification \u2461 et la pr\u00e9vision \u2462.\n[NOTES] lue via get_articles_arretes_avec_stock_neshu, get_point_commande_neshu ; 16 route(s) API ; \u00e9crans : _conformite, _pilotage, _planappro, accueil, groupe, neshu_conformite, neshu_controles, neshu_partenaires, neshu_pilotage, neshu_prevision, neshu_stocks, neshu_terrain, supply, v2_nav, v2_system (relev\u00e9 du 2026-10-08, tools/audit/carte_donnees.py de l'app). Les r\u00e8gles de l'app (statuts, p\u00e9riodes, exclusions) restent appliqu\u00e9es par l'app ; les filtres repris ici sont identiques et sans perte. R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

-- Point de commande Neshu (« Commandes à passer »), pour Cockpit Supply : seules les colonnes lues par l'app.
select
    company_id,
    product_id,
    company_code,
    product_code,
    product_name,
    product_family,
    statut_vie,
    classe_abc,
    methode_prevision,
    unite_commande_code,
    delai_jours,
    z_service,
    demande_prevue_mensuelle,
    demande_prevue_journaliere,
    sigma_demande_mensuelle,
    stock_actuel,
    encours_fournisseur,
    position_stock,
    stock_securite,
    point_commande,
    semaines_surstock_max,
    stock_max,
    statut_reappro,
    quantite_excedentaire,
    coeff_conditionnement,
    quantite_a_commander,
    quantite_a_commander_conditionnee,
    date_calcul,
    is_article_arrete_avec_stock
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__point_commande_neshu`
    );
  