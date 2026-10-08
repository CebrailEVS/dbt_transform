
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__lcdp_entree_fabrication`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Entr\u00e9es en fabrication (torr\u00e9faction) Caf\u00e9s du Phare, tel que lu par l'application Cockpit Supply.\n[COMMENT CONSTRUITE] int_oracle_lcdp__entree_fabrication_tasks r\u00e9duite aux colonnes r\u00e9ellement utilis\u00e9es par l'app (relev\u00e9 du code, aucune utilisation de la table enti\u00e8re). Toutes les lignes.\n[GRAIN] 1 ligne par task_product_id (ligne produit d'une entr\u00e9e en fabrication). ~1 380 lignes (~208 t\u00e2ches, 39 caf\u00e9s verts), depuis janvier 2025.\n[NOTES] lue via get_mouvements_fabrication_lcdp ; 14 route(s) API ; \u00e9crans : _flux, lcdp_pilotage, lcdp_torrefaction, supply (relev\u00e9 du 2026-10-08, tools/audit/carte_donnees.py de l'app). Les r\u00e8gles de l'app (statuts, p\u00e9riodes, exclusions) restent appliqu\u00e9es par l'app ; les filtres repris ici sont identiques et sans perte. R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

-- Entrées en fabrication (torréfaction) Cafés du Phare, pour Cockpit Supply : seules les colonnes lues par l'app.
select
    task_product_id,
    task_id,
    product_code,
    task_start_date,
    source_code,
    destination_code,
    quantity,
    valuation
from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__entree_fabrication_tasks`
    );
  