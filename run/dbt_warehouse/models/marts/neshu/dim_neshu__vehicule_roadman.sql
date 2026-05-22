
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`dim_neshu__vehicule_roadman`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Dimension v\u00e9hicule-roadman : pivot v\u00e9hicule avec son roadman associ\u00e9 et son code GEA.\n[COMMENT CONSTRUITE] Issu de stg_oracle_neshu__resources filtr\u00e9 sur les v\u00e9hicules, joint au roadman associ\u00e9 via resources_idresources, enrichi du code GEA depuis ref_oracle_neshu__roadman_gea (seed reference).\n[GRAIN] 1 ligne par v\u00e9hicule (resources_vehicule_id).\n[NOTES] P\u00e9rim\u00e8tre partiellement redondant avec dim_neshu__resource (qui couvre PERSON + VEHICULE) \u2014 usage sp\u00e9cifique pour les facts orient\u00e9s v\u00e9hicule. \u00c0 clarifier lors d'une refacto ult\u00e9rieure.\n"""
    )
    as (
      

select
    r.idresources as resources_vehicule_id,
    r.code as vehicule_code,
    r.resources_idresources as resources_roadman_id,
    r.name as roadman_code,
    r.created_at,
    r.updated_at,
    g.gea_code
from `prod_staging.stg_oracle_neshu__resources` as r
left join `evs-datastack-prod`.`prod_reference`.`ref_oracle_neshu__roadman_gea` as g
    on r.name = g.roadman_code
where r.idresources_type = 3
    );
  