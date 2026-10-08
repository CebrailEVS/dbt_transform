
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__lcdp_disponibilite_depot_mensuel`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Disponibilit\u00e9 mensuelle des articles dans les d\u00e9p\u00f4ts Caf\u00e9s du Phare, tel que lu par l'application Cockpit Supply.\n[COMMENT CONSTRUITE] fct_supply_chain__disponibilite_article_lcdp_depot_mensuel r\u00e9duite aux colonnes r\u00e9ellement utilis\u00e9es par l'app (relev\u00e9 du code, aucune utilisation de la table enti\u00e8re). Toutes les lignes.\n[GRAIN] 1 ligne par (mois, d\u00e9p\u00f4t, article).\n[NOTES] lue via get_disponibilite_lcdp ; 2 route(s) API ; \u00e9crans : supply (relev\u00e9 du 2026-10-08, tools/audit/carte_donnees.py de l'app). Les r\u00e8gles de l'app (statuts, p\u00e9riodes, exclusions) restent appliqu\u00e9es par l'app ; les filtres repris ici sont identiques et sans perte. R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

-- Disponibilité mensuelle des articles dans les dépôts Cafés du Phare, pour Cockpit Supply : seules les colonnes lues par l'app.
select
    mois,
    company_id,
    entity_code,
    product_code,
    taux_disponibilite_pct
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__disponibilite_article_lcdp_depot_mensuel`
    );
  