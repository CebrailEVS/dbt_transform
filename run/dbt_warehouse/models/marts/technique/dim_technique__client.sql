
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`dim_technique__client`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER]\nDimension client Yuman \u2014 conformed entre tous les partenaires EVS hors\nNespresso (Neshu, Brita, Auum, ...). D\u00e9crit qui sont les clients chez qui\nEVS intervient techniquement via la plateforme Yuman.\n\n[COMMENT CONSTRUITE]\nLecture directe de `stg_yuman__clients`. S\u00e9lection des colonnes utiles\nau BI : code, nom, cat\u00e9gorie, adresse, partenaire, \u00e9tat actif.\n\n[GRAIN]\n1 ligne par `client_id` (PK Yuman).\n\n[NOTES]\nConformed dim \u2014 r\u00e9f\u00e9renc\u00e9e par les facts `fct_technique__*`, et aussi\npar certains marts `fct_neshu__*` (workorder_delai notamment).\n"""
    )
    as (
      

select
    client_id,
    partner_name,
    client_code,
    client_name,
    client_category,
    client_address,
    is_active as client_is_active,
    created_at,
    updated_at

from `evs-datastack-prod`.`prod_staging`.`stg_yuman__clients`
    );
  