
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__neshu_couverture_stock`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Couverture de stock des d\u00e9p\u00f4ts Neshu, tel que lu par l'application Cockpit Supply.\n[COMMENT CONSTRUITE] fct_supply_chain__couverture_stock_neshu r\u00e9duite aux colonnes r\u00e9ellement utilis\u00e9es par l'app (relev\u00e9 du code, aucune utilisation de la table enti\u00e8re). Toutes les lignes.\n[GRAIN] 1 ligne par (date_calcul, company_id, product_code).\n[NOTES] lue via get_couverture ; \u00e9crans : _conformite, _pilotage, accueil, groupe, neshu_conformite, neshu_controles, neshu_partenaires, neshu_pilotage, neshu_prevision, neshu_stocks, neshu_terrain, supply, v2_nav, v2_system. Les r\u00e8gles de l'app (statuts, p\u00e9riodes, exclusions) restent appliqu\u00e9es par l'app ; les filtres repris ici sont identiques et sans perte. R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

-- Couverture de stock des dépôts Neshu, pour Cockpit Supply : seules les colonnes lues par l'app.
select
    date_calcul,
    company_id,
    entity_code,
    entity_name,
    product_code,
    product_name,
    stock_actuel,
    conso_journaliere_n1,
    conso_mensuelle_moy_n1,
    jours_couverture,
    qte_a_commander
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__couverture_stock_neshu`
    );
  