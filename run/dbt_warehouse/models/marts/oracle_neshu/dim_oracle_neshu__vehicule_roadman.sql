
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__vehicule_roadman`
      
    
    

    
    OPTIONS(
      description="""Dimension regroupant les v\u00e9hicules et roadmen issus de la table `stg_oracle_neshu__resources`, enrichie avec le code GEA provenant de la r\u00e9f\u00e9rence `ref_oracle_neshu__roadman_gea`.\n"""
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
  