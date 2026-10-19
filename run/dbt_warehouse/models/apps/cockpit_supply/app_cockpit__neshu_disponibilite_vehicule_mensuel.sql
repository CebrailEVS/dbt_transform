
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__neshu_disponibilite_vehicule_mensuel`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Disponibilit\u00e9 mensuelle des articles dans les v\u00e9hicules Neshu, tel que lu par l'application Cockpit Supply.\n[COMMENT CONSTRUITE] Agr\u00e9gat mensuel de int_oracle_neshu__stock_theorique sur les v\u00e9hicules : jours observ\u00e9s et jours sans rupture ; libell\u00e9s et statut du v\u00e9hicule du dernier jour du mois. M\u00eame calcul que fct_supply_chain__disponibilite_article_neshu_vehicule_mensuel.\n[GRAIN] 1 ligne par (mois, v\u00e9hicule, article).\n[NOTES] lue via get_disponibilite_vehicule ; \u00e9crans : neshu_terrain. Les r\u00e8gles de l'app (statuts, p\u00e9riodes, exclusions) restent appliqu\u00e9es par l'app ; les filtres repris ici sont identiques et sans perte. R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

-- Disponibilité mensuelle des articles dans les véhicules Neshu, pour Cockpit Supply, construite depuis
-- l'intermediate. Libellés et statut du véhicule pris au dernier jour du mois ; ce statut est l'état COURANT
-- du véhicule et ne filtre pas.
select
    date_trunc(date(date_system), month) as mois,
    id_entity as resources_id,
    any_value(entity_code having max date_system) as entity_code,
    any_value(entity_name having max date_system) as entity_name,
    product_code,
    any_value(product_name having max date_system) as product_name,
    count(distinct date(date_system)) as jours_observes,
    count(distinct case when is_out_of_stock = false then date(date_system) end) as jours_disponibles,
    any_value(is_vehicle_active having max date_system) as is_vehicle_active
from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__stock_theorique`
where entity_type = 'resource'
group by date_trunc(date(date_system), month), id_entity, product_code
    );
  