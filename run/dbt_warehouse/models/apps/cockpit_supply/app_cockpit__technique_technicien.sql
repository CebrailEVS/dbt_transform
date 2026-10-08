
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__technique_technicien`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] R\u00e9f\u00e9rentiel des techniciens TechCare, tel que lu par l'application Cockpit Supply.\n[COMMENT CONSTRUITE] dim_technique__technician r\u00e9duite aux colonnes r\u00e9ellement utilis\u00e9es par l'app (relev\u00e9 du code, aucune utilisation de la table enti\u00e8re). Toutes les lignes.\n[GRAIN] 1 ligne par `user_id` (PK Yuman du technicien).\n[NOTES] lue via get_dim_technicians ; 35 route(s) API ; \u00e9crans : _conformite, _flux, _pilotage, _planappro, accueil, groupe, supply, technique_partenaires, technique_pilotage, technique_stocks, technique_terrain, v2_nav (relev\u00e9 du 2026-10-08, tools/audit/carte_donnees.py de l'app). Les r\u00e8gles de l'app (statuts, p\u00e9riodes, exclusions) restent appliqu\u00e9es par l'app ; les filtres repris ici sont identiques et sans perte. R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

-- Référentiel des techniciens TechCare, pour Cockpit Supply : seules les colonnes lues par l'app.
select
    user_id,
    user_name,
    entrepot_rattachement,
    is_active,
    user_type
from `evs-datastack-prod`.`prod_marts`.`dim_technique__technician`
    );
  