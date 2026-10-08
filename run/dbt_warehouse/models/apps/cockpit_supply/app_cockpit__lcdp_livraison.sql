
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__lcdp_livraison`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Livraisons Caf\u00e9s du Phare (bons de livraison), tel que lu par l'application Cockpit Supply.\n[COMMENT CONSTRUITE] int_oracle_lcdp__livraison_tasks r\u00e9duite aux colonnes r\u00e9ellement utilis\u00e9es par l'app (relev\u00e9 du code, aucune utilisation de la table enti\u00e8re). Lignes : t\u00e2ches valid\u00e9es seulement (FAIT, VALIDE), comme l'app.\n[GRAIN] 1 ligne par task_product_id (ligne produit d'un bon de livraison). ~8,4k lignes (~3,3k bons), depuis fin 2024.\n[NOTES] lue via get_livraison_lcdp ; 21 route(s) API ; \u00e9crans : _flux, _pilotage, accueil, lcdp_distrib, lcdp_torrefaction, supply (relev\u00e9 du 2026-10-08, tools/audit/carte_donnees.py de l'app). Les r\u00e8gles de l'app (statuts, p\u00e9riodes, exclusions) restent appliqu\u00e9es par l'app ; les filtres repris ici sont identiques et sans perte. R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

-- Livraisons Cafés du Phare (bons de livraison), pour Cockpit Supply : seules les colonnes lues par l'app,
-- tâches validées seulement (FAIT, VALIDE), comme l'app.
select
    task_product_id,
    task_id,
    company_id,
    product_source_id,
    product_code,
    task_status_code,
    task_start_date,
    quantity,
    valuation
from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__livraison_tasks`
where task_status_code in ('FAIT', 'VALIDE')
    );
  