
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__neshu_erreur_prevision`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Erreur de pr\u00e9vision mensuelle des articles Neshu, tel que lu par l'application Cockpit Supply.\n[COMMENT CONSTRUITE] fct_supply_chain__erreur_prevision_neshu r\u00e9duite aux colonnes r\u00e9ellement utilis\u00e9es par l'app (relev\u00e9 du code, aucune utilisation de la table enti\u00e8re). Toutes les lignes.\n[GRAIN] 1 ligne par (mois_cible, company_id, product_id) \u2014 full rebuild quotidien.\n[NOTES] lue via get_erreur_prevision_neshu ; \u00e9crans : _conformite, groupe, neshu_prevision, supply, v2_nav. Les r\u00e8gles de l'app (statuts, p\u00e9riodes, exclusions) restent appliqu\u00e9es par l'app ; les filtres repris ici sont identiques et sans perte. R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

-- Erreur de prévision mensuelle des articles Neshu, pour Cockpit Supply : seules les colonnes lues par l'app.
select
    mois_cible,
    company_id,
    product_id,
    company_code,
    product_code,
    product_name,
    classe_abc,
    methode_prevision,
    nb_mois_anciennete,
    demande_reelle,
    prevision_moyenne_mobile,
    prevision_saisonniere,
    prevision_naive,
    erreur,
    erreur_absolue
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__erreur_prevision_neshu`
    );
  