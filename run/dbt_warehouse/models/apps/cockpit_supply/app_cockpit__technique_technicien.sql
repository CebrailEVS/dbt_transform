
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__technique_technicien`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] R\u00e9f\u00e9rentiel des techniciens TechCare, tel que lu par l'application Cockpit Supply.\n[COMMENT CONSTRUITE] stg_yuman__users filtr\u00e9e sur les techniciens (type technician, ou manager d\u00e9clar\u00e9 technicien), jointe \u00e0 stg_yuman__storehouses ; colonnes lues par l'app, dont le d\u00e9p\u00f4t de rattachement et le nom du stock du technicien (rattachement des mouvements \u00ab ST - \u2026 \u00bb au d\u00e9p\u00f4t, \u00e0 la place de l'import manuel \u00ab Export des utilisateurs \u00bb). Toutes les lignes.\n[GRAIN] 1 ligne par `user_id` (PK Yuman du technicien).\n[NOTES] lue via get_dim_technicians ; \u00e9crans : _conformite, _flux, _pilotage, _planappro, accueil, groupe, supply, technique_partenaires, technique_pilotage, technique_stocks, technique_terrain, v2_nav. Les r\u00e8gles de l'app (statuts, p\u00e9riodes, exclusions) restent appliqu\u00e9es par l'app ; les filtres repris ici sont identiques et sans perte. R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

-- Référentiel des techniciens TechCare pour Cockpit Supply, construit depuis le staging.
-- Technicien = utilisateur de type technician, ou manager déclaré technicien. L'entrepôt (camion) du
-- technicien porte le même identifiant que l'utilisateur dans Yuman.
select
    u.user_id,
    u.user_name,
    u.entrepot_rattachement,
    s.storehouses_name,
    u.is_active,
    u.user_type
from `evs-datastack-prod`.`prod_staging`.`stg_yuman__users` as u
left join `evs-datastack-prod`.`prod_staging`.`stg_yuman__storehouses` as s
    on u.user_id = s.storehouses_id
where
    u.user_type = 'technician'
    or (u.user_type = 'manager' and u.is_manager_as_technician = true)
    );
  