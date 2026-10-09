
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`dim_technique__technician`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER]\nDimension technicien EVS \u2014 chaque utilisateur Yuman ayant un r\u00f4le\ntechnique (techniciens purs + managers intervenant aussi comme\ntechnicien).\n\n[COMMENT CONSTRUITE]\nLecture de `stg_yuman__users` filtr\u00e9 sur user_type IN ('technician',\n'manager') avec flag `is_manager_as_technician`. Enrichie avec :\n- le nom du manager direct (via self-join sur stg_yuman__users)\n- le d\u00e9p\u00f4t via deux angles compl\u00e9mentaires : `storehouses_name` = stock\n  nominatif individuel du technicien ('ST - NOM'), via jointure\n  `storehouses_id = user_id` sur `stg_yuman__storehouses` ;\n  `entrepot_rattachement` = atelier/d\u00e9p\u00f4t physique parent regroupant les\n  techniciens par site (ex. '06 - ATELIER RUNGIS DEPOT'), issu du champ\n  personnalis\u00e9 Yuman.\n\n[GRAIN]\n1 ligne par `user_id` (PK Yuman du technicien).\n\n[NOTES]\nConformed dim \u2014 utilis\u00e9e par les facts intervention/pricing. Inclut\nles snapshots history via `snap_yuman__users` en aval si besoin SCD2.\n"""
    )
    as (
      

with source as (
    select * from `evs-datastack-prod`.`prod_staging`.`stg_yuman__users`
),

technicians as (
    select *
    from source
    where
        user_type = 'technician'
        or (user_type = 'manager' and is_manager_as_technician = true)
),

storehouses as (
    select * from `evs-datastack-prod`.`prod_staging`.`stg_yuman__storehouses`
),

final as (
    select
        -- ids
        t.user_id,
        t.nomad_id,
        t.manager_id,

        -- attributes
        t.user_name,
        t.user_email,
        t.user_phone,
        t.user_type,
        t.user_secteur,
        t.entrepot_rattachement,
        m.user_name as manager_name,
        s.storehouses_name,

        -- flags
        t.is_active,
        t.is_manager_as_technician,

        -- metadata
        t.created_at,
        t.updated_at
    from technicians as t
    left join source as m
        on t.manager_id = m.user_id
    left join storehouses as s
        on t.user_id = s.storehouses_id
)

select * from final
    );
  