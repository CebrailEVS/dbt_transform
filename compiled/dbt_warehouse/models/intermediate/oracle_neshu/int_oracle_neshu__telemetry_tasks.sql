

with telemetry_tasks as (
    select
        -- PK naturelle de task_has_product
        thp.idtask_has_product as task_product_id,
        
        -- IDs business
        t.idtask as task_id,
        t.iddevice as device_id,
        t.idcompany_peer as company_id,
        thp.idproduct as product_id,
        t.idlocation as location_id,
        
        -- Codes métier pour les jointures futures
        d.code as device_code,
        thp.code as product_code,
        c.code as company_code,
        
        -- Données métier
        c.name as company_name,
        l.access_info as task_location_info,
        
        -- Dates business
        t.real_start_date as task_start_date,
        
        -- Métrique business
        CAST(1 AS INT64) AS telemetry_quantity,  -- 1 Tâche = 1 unité de télémétrie
        
        -- Timestamps techniques pour l'incrément
        t.updated_at,
        t.created_at,
        t.extracted_at
        
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task` t
    
    -- Jointure obligatoire pour récupérer les produits
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_has_product` thp
        on thp.idtask = t.idtask
    
    -- Jointures pour enrichissement
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__company` c
        on c.idcompany = t.idcompany_peer
        
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__device` d
        on d.iddevice = t.iddevice
        
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__location` l
        on l.idlocation = t.idlocation
    
    -- Jointures pour le filtrage sur labels télémétrie
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_has_task` lht
        on t.idtask = lht.idtask
        
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label` la
        on lht.idlabel = la.idlabel
        
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_family` lf
        on la.idlabel_family = lf.idlabel_family
    
    where 1=1
        -- Filtres business critiques
        and t.idtask_status in (1, 4)  -- FAIT et VALIDE uniquement
        and t.code_status_record = '1'   -- Enregistrement actif (string)
        and t.idtask_type = 3           -- Type télémétrie
        and lf.code = 'TELEM_SOURCE'    -- Label famille télémétrie source
        
        -- Filtre qualité données
        and t.real_start_date is not null  -- Éviter les tâches sans date de début
)

select * from telemetry_tasks


    where updated_at >= (
        select max(updated_at) - interval 1 day
        from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__telemetry_tasks`
    )
