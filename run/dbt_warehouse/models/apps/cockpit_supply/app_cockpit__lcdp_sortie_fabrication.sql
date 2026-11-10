
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__lcdp_sortie_fabrication`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Sorties de fabrication (torr\u00e9faction) Caf\u00e9s du Phare, tel que lu par l'application Cockpit Supply.\n[COMMENT CONSTRUITE] int_oracle_lcdp__sortie_fabrication_tasks r\u00e9duite aux colonnes r\u00e9ellement utilis\u00e9es par l'app (relev\u00e9 du code, aucune utilisation de la table enti\u00e8re). Lignes : t\u00e2ches valid\u00e9es (FAIT, VALIDE) qui sortent de FABRICATION, vers TORREFIEVRAC, LOG ou CASSE ; une sortie saisie pour un simple transfert (is_transfert) est compt\u00e9e dans app_cockpit__lcdp_livraison_interne.\n[GRAIN] 1 ligne par task_product_id (ligne produit d'une sortie de fabrication).\n[NOTES] lue via get_mouvements_fabrication_lcdp ; \u00e9crans : _flux, lcdp_pilotage, lcdp_torrefaction, supply. Les r\u00e8gles de l'app (statuts, p\u00e9riodes, exclusions) restent appliqu\u00e9es par l'app ; les filtres repris ici sont identiques et sans perte. R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

-- Sorties de fabrication (torréfaction) Cafés du Phare, pour Cockpit Supply : seules les colonnes lues par l'app,
-- tâches validées (FAIT, VALIDE) qui sortent bien de FABRICATION ; une sortie saisie pour un simple transfert
-- est comptée en livraison interne (app_cockpit__lcdp_livraison_interne).
select
    task_product_id,
    task_id,
    product_code,
    timestamp(datetime(task_start_date, 'Europe/Paris')) as task_start_date,
    source_code,
    destination_code,
    quantity,
    valuation
from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__sortie_fabrication_tasks`
where
    task_status_code in ('FAIT', 'VALIDE')
    and not is_transfert
    );
  