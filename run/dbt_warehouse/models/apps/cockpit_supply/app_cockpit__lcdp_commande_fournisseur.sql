
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__lcdp_commande_fournisseur`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Commandes fournisseurs Caf\u00e9s du Phare (Distrilog), tel que lu par l'application Cockpit Supply.\n[COMMENT CONSTRUITE] int_oracle_lcdp__commande_fournisseur_tasks r\u00e9duite aux m\u00eames colonnes que app_cockpit__neshu_commande_fournisseur. Lignes : t\u00e2ches valid\u00e9es seulement (FAIT, VALIDE), comme pour Neshu ; toutes les commandes, livr\u00e9es ou non (taux de service fournisseur).\n[GRAIN] 1 ligne par task_product_id (ligne produit d'une commande fournisseur).\n[NOTES] Taux de service des fiches fournisseurs Caf\u00e9s du Phare (lot ISO P1, demande du 09/10/2026) ; destination_code LOG = Distribution Auto. Les r\u00e8gles de l'app (statuts, p\u00e9riodes, exclusions) restent appliqu\u00e9es par l'app. R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

-- Commandes fournisseurs Cafés du Phare, pour Cockpit Supply (taux de service des fiches fournisseurs,
-- lot ISO P1) : mêmes colonnes et même filtre que app_cockpit__neshu_commande_fournisseur,
-- tâches validées seulement (FAIT, VALIDE) ; toutes les commandes, livrées ou non.
select
    task_product_id,
    task_id,
    company_id,
    destination_code,
    product_code,
    task_status_code,
    delivery_status_code,
    timestamp(datetime(task_start_date, 'Europe/Paris')) as task_start_date,
    unit_coeff_multi,
    unit_coeff_div,
    quantity,
    valuation
from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__commande_fournisseur_tasks`
where task_status_code in ('FAIT', 'VALIDE')
    );
  