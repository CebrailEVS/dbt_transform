
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__lcdp_livraison_interne`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Livraisons internes Caf\u00e9s du Phare, tel que lu par l'application Cockpit Supply.\n[COMMENT CONSTRUITE] int_oracle_lcdp__livraison_interne_tasks r\u00e9duite aux colonnes r\u00e9ellement utilis\u00e9es par l'app (relev\u00e9 du code, aucune utilisation de la table enti\u00e8re). Lignes : t\u00e2ches valid\u00e9es seulement (FAIT, VALIDE), comme l'app.\n[GRAIN] 1 ligne par task_product_id (ligne produit d'un mouvement interne).\n[NOTES] lue via get_livraison_interne_lcdp ; \u00e9crans : _flux, _pilotage, accueil, lcdp_distrib, lcdp_torrefaction, supply. Les r\u00e8gles de l'app (statuts, p\u00e9riodes, exclusions) restent appliqu\u00e9es par l'app ; les filtres repris ici sont identiques et sans perte. R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

-- Livraisons internes Cafés du Phare, pour Cockpit Supply : seules les colonnes lues par l'app,
-- tâches validées seulement (FAIT, VALIDE), comme l'app.
select
    task_product_id,
    task_id,
    task_start_date,
    task_status_code,
    source_code,
    destination_code,
    product_code,
    quantity,
    valuation
from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__livraison_interne_tasks`
where task_status_code in ('FAIT', 'VALIDE')
    );
  