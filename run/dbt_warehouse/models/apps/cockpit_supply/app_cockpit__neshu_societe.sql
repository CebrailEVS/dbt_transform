
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__neshu_societe`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] R\u00e9f\u00e9rentiel des soci\u00e9t\u00e9s Neshu (clients, fournisseurs, d\u00e9p\u00f4ts), tel que lu par l'application Cockpit Supply.\n[COMMENT CONSTRUITE] dim_neshu__company r\u00e9duite aux colonnes r\u00e9ellement utilis\u00e9es par l'app (relev\u00e9 du code, aucune utilisation de la table enti\u00e8re). Toutes les lignes.\n[GRAIN] 1 ligne par company_id.\n[NOTES] lue via get_commande_fournisseur, get_dim_company, get_livraison, get_reception ; \u00e9crans : _conformite, _flux, _pilotage, _planappro, accueil, groupe, lcdp_distrib, neshu_conformite, neshu_controles, neshu_partenaires, neshu_pilotage, neshu_prevision, neshu_stocks, neshu_terrain, nunshen_partenaires, supply, technique_partenaires, v2_nav, v2_system. Les r\u00e8gles de l'app (statuts, p\u00e9riodes, exclusions) restent appliqu\u00e9es par l'app ; les filtres repris ici sont identiques et sans perte. R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

-- Référentiel des sociétés Neshu (clients, fournisseurs, dépôts), pour Cockpit Supply : seules les colonnes lues par l'app.
select
    company_id,
    company_code,
    company_name,
    is_depot
from `evs-datastack-prod`.`prod_marts`.`dim_neshu__company`
    );
  