
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__neshu_disponibilite_vehicule_mensuel`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Disponibilit\u00e9 mensuelle des articles dans les v\u00e9hicules Neshu, tel que lu par l'application Cockpit Supply.\n[COMMENT CONSTRUITE] fct_supply_chain__disponibilite_article_neshu_vehicule_mensuel r\u00e9duite aux colonnes r\u00e9ellement utilis\u00e9es par l'app (relev\u00e9 du code, aucune utilisation de la table enti\u00e8re). Toutes les lignes.\n[GRAIN] 1 ligne par (mois, v\u00e9hicule, article).\n[NOTES] lue via get_disponibilite_vehicule ; \u00e9crans : neshu_terrain. Les r\u00e8gles de l'app (statuts, p\u00e9riodes, exclusions) restent appliqu\u00e9es par l'app ; les filtres repris ici sont identiques et sans perte. R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

-- Disponibilité mensuelle des articles dans les véhicules Neshu, pour Cockpit Supply : seules les colonnes lues par l'app.
select
    mois,
    resources_id,
    entity_code,
    entity_name,
    product_code,
    product_name,
    jours_observes,
    jours_disponibles,
    is_vehicle_active
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__disponibilite_article_neshu_vehicule_mensuel`
    );
  