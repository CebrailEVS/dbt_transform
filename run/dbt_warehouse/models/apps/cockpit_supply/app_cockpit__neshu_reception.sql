
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__neshu_reception`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] R\u00e9ceptions fournisseurs Neshu, tel que lu par l'application Cockpit Supply.\n[COMMENT CONSTRUITE] int_oracle_neshu__reception_tasks r\u00e9duite aux colonnes r\u00e9ellement utilis\u00e9es par l'app (relev\u00e9 du code, aucune utilisation de la table enti\u00e8re). Lignes : t\u00e2ches valid\u00e9es seulement (FAIT, VALIDE), comme l'app.\n[GRAIN] 1 ligne par task_product_id (ligne produit d'un bon de r\u00e9ception).\n[NOTES] lue via get_reception ; \u00e9crans : _conformite, _flux, accueil, groupe, lcdp_distrib, neshu_conformite, neshu_partenaires, neshu_pilotage, neshu_stocks, neshu_terrain, nunshen_partenaires, supply, technique_partenaires, v2_nav. Les r\u00e8gles de l'app (statuts, p\u00e9riodes, exclusions) restent appliqu\u00e9es par l'app ; les filtres repris ici sont identiques et sans perte. R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

-- Réceptions fournisseurs Neshu, pour Cockpit Supply : seules les colonnes lues par l'app,
-- tâches validées seulement (FAIT, VALIDE), comme l'app.
select
    task_product_id,
    task_id,
    company_id,
    destination_code,
    product_code,
    task_status_code,
    task_start_date,
    delivery_lead_time_days,
    is_lead_time_valid,
    quantity,
    valuation
from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__reception_tasks`
where task_status_code in ('FAIT', 'VALIDE')
    );
  