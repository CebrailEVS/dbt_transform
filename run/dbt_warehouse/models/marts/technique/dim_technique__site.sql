
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`dim_technique__site`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER]\nDimension site Yuman \u2014 adresses physiques o\u00f9 sont install\u00e9es les machines\ndes clients EVS. Un client peut avoir plusieurs sites.\n\n[COMMENT CONSTRUITE]\nLecture directe de `stg_yuman__sites`. Inclut le rattachement\n`client_id` (FK) et `agency_id` (agence EVS responsable du site).\n\n[GRAIN]\n1 ligne par `site_id` (PK Yuman).\n"""
    )
    as (
      

select
    site_id,
    client_id,
    agency_id,
    site_code,
    site_name,
    site_address,
    site_postal_code,
    created_at,
    updated_at

from `evs-datastack-prod`.`prod_staging`.`stg_yuman__sites`
    );
  