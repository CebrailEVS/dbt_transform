{{
  config(
    materialized='table',
    description='Dimension Vehicules et Roadman pour les taches'
  )
}}

select 
    r.idresources as id_res_vehicule,
    r.code as code_vehicule,
    r.resources_idresources as id_res_roadman, 
    r.name as code_roadman,
    r.created_at,
    r.updated_at,
    g.gea_code 
from `prod_staging.stg_oracle_neshu__resources` r
left join {{ ref('ref_oracle_neshu__roadman_gea')}} g 
    on g.roadman_code = r.name
where r.idresources_type = 3