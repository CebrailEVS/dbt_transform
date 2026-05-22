

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