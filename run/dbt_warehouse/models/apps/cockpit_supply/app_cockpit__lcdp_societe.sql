
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__lcdp_societe`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] R\u00e9f\u00e9rentiel des soci\u00e9t\u00e9s Caf\u00e9s du Phare, tel que lu par l'application Cockpit Supply.\n[COMMENT CONSTRUITE] stg_oracle_lcdp__company filtr\u00e9e comme la dim (types 1, 2, 4 et 6). Colonnes lues par l'app seulement.\n[GRAIN] 1 ligne par company_id.\n[NOTES] lue via get_livraison_lcdp, get_reception_lcdp ; \u00e9crans : _conformite, _flux, _pilotage, accueil, groupe, lcdp_distrib, lcdp_pilotage, lcdp_torrefaction, neshu_partenaires, nunshen_partenaires, supply, technique_partenaires, v2_nav. Les r\u00e8gles de l'app (statuts, p\u00e9riodes, exclusions) restent appliqu\u00e9es par l'app ; les filtres repris ici sont identiques et sans perte. R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

-- Référentiel des sociétés Cafés du Phare pour Cockpit Supply, construit depuis le staging.
-- Périmètre : fournisseurs (1), clients (2), société interne (4) et dépôts (6).
select
    idcompany as company_id,
    code as company_code,
    name as company_name
from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__company`
where idcompany_type in (1, 2, 4, 6)
    );
  