
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__lcdp_livraison_interne`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Livraisons internes Caf\u00e9s du Phare, tel que lu par l'application Cockpit Supply.\n[COMMENT CONSTRUITE] int_oracle_lcdp__livraison_interne_tasks r\u00e9duite aux colonnes r\u00e9ellement utilis\u00e9es par l'app (relev\u00e9 du code, aucune utilisation de la table enti\u00e8re). Lignes : t\u00e2ches valid\u00e9es seulement (FAIT, VALIDE), comme l'app. S'y ajoutent les t\u00e2ches de fabrication valid\u00e9es saisies pour un simple transfert (is_transfert de int_oracle_lcdp__sortie_fabrication_tasks et int_oracle_lcdp__entree_fabrication_tasks), rep\u00e9r\u00e9es par type_tache.\n[GRAIN] 1 ligne par task_product_id (ligne produit d'un mouvement interne).\n[NOTES] lue via get_livraison_interne_lcdp ; \u00e9crans : _flux, _pilotage, accueil, lcdp_distrib, lcdp_torrefaction, supply. Les r\u00e8gles de l'app (statuts, p\u00e9riodes, exclusions) restent appliqu\u00e9es par l'app ; les filtres repris ici sont identiques et sans perte. R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

-- Livraisons internes Cafés du Phare, pour Cockpit Supply : seules les colonnes lues par l'app, tâches validées
-- seulement (FAIT, VALIDE), comme l'app. S'y ajoutent les tâches de fabrication saisies pour un simple transfert
-- (is_transfert) : elles déplacent du stock sans torréfaction, l'app les compte donc comme des transferts.
with mouvements as (
    select
        task_product_id,
        task_id,
        task_start_date,
        task_status_code,
        source_code,
        destination_code,
        product_code,
        quantity,
        valuation,
        'LIVRAISON_INTERNE' as type_tache
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__livraison_interne_tasks`
    where task_status_code in ('FAIT', 'VALIDE')

    union all

    select
        task_product_id,
        task_id,
        task_start_date,
        task_status_code,
        source_code,
        destination_code,
        product_code,
        quantity,
        valuation,
        'SORTIE_FABRICATION' as type_tache
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__sortie_fabrication_tasks`
    where task_status_code in ('FAIT', 'VALIDE') and is_transfert

    union all

    select
        task_product_id,
        task_id,
        task_start_date,
        task_status_code,
        source_code,
        destination_code,
        product_code,
        quantity,
        valuation,
        'ENTREE_FABRICATION' as type_tache
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__entree_fabrication_tasks`
    where task_status_code in ('FAIT', 'VALIDE') and is_transfert
)

select
    task_product_id,
    task_id,
    timestamp(datetime(task_start_date, 'Europe/Paris')) as task_start_date,
    task_status_code,
    source_code,
    destination_code,
    product_code,
    quantity,
    valuation,
    type_tache
from mouvements
    );
  