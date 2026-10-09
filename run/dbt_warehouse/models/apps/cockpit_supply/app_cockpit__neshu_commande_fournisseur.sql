
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__neshu_commande_fournisseur`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Commandes fournisseurs Neshu, tel que lu par l'application Cockpit Supply.\n[COMMENT CONSTRUITE] int_oracle_neshu__commande_fournisseur_tasks r\u00e9duite aux colonnes r\u00e9ellement utilis\u00e9es par l'app (relev\u00e9 du code, aucune utilisation de la table enti\u00e8re). Lignes : t\u00e2ches valid\u00e9es seulement (FAIT, VALIDE), comme l'app ; toutes les commandes, livr\u00e9es ou non (taux de service fournisseur).\n[GRAIN] 1 ligne par task_product_id (ligne produit d'une commande fournisseur).\n[NOTES] lue via get_commande_fournisseur ; \u00e9crans : _conformite, _pilotage, _planappro, accueil, groupe, lcdp_distrib, neshu_conformite, neshu_controles, neshu_partenaires, neshu_pilotage, neshu_prevision, neshu_stocks, neshu_terrain, nunshen_partenaires, supply, technique_partenaires, v2_nav, v2_system. Les r\u00e8gles de l'app (statuts, p\u00e9riodes, exclusions) restent appliqu\u00e9es par l'app ; les filtres repris ici sont identiques et sans perte. R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

-- Commandes fournisseurs Neshu, pour Cockpit Supply : seules les colonnes lues par l'app,
-- tâches validées seulement (FAIT, VALIDE), comme l'app ; toutes les commandes, livrées ou non (taux de service fournisseur).
select
    task_product_id,
    task_id,
    company_id,
    destination_code,
    product_code,
    task_status_code,
    delivery_status_code,
    task_start_date,
    unit_coeff_multi,
    unit_coeff_div,
    quantity,
    valuation
from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__commande_fournisseur_tasks`
where task_status_code in ('FAIT', 'VALIDE')
    );
  