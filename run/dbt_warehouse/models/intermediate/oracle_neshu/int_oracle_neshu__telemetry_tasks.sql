-- back compat for old kwarg name
  
  
        
            
	    
	    
            
        
    

    

    merge into `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__telemetry_tasks` as DBT_INTERNAL_DEST
        using (

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
        cast(1 as int64) as telemetry_quantity,  -- 1 Tâche = 1 unité de télémétrie

        -- Timestamps techniques pour l'incrément
        t.updated_at,
        t.created_at,
        t.extracted_at

    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task` as t

    -- Jointure obligatoire pour récupérer les produits
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_has_product` as thp
        on t.idtask = thp.idtask

    -- Jointures pour enrichissement
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__company` as c
        on t.idcompany_peer = c.idcompany

    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__device` as d
        on t.iddevice = d.iddevice

    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__location` as l
        on t.idlocation = l.idlocation

    -- Jointures pour le filtrage sur labels télémétrie
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_has_task` as lht
        on t.idtask = lht.idtask

    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label` as la
        on lht.idlabel = la.idlabel

    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_family` as lf
        on la.idlabel_family = lf.idlabel_family

    where 1 = 1
    -- Filtres business critiques
    and t.idtask_status in (1, 4)  -- FAIT et VALIDE uniquement
    and t.code_status_record = '1'   -- Enregistrement actif (string)
    and t.idtask_type = 3           -- Type télémétrie
    and lf.code = 'TELEM_SOURCE'    -- Label famille télémétrie source

    -- Filtre qualité données
    and t.real_start_date is not null  -- Éviter les tâches sans date de début
)

select * from telemetry_tasks


    where telemetry_tasks.updated_at >= (
        select max(t.updated_at) - interval 1 day
        from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__telemetry_tasks` as t
    )

        ) as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.task_product_id = DBT_INTERNAL_DEST.task_product_id))

    
    when matched then update set
        `task_product_id` = DBT_INTERNAL_SOURCE.`task_product_id`,`task_id` = DBT_INTERNAL_SOURCE.`task_id`,`device_id` = DBT_INTERNAL_SOURCE.`device_id`,`company_id` = DBT_INTERNAL_SOURCE.`company_id`,`product_id` = DBT_INTERNAL_SOURCE.`product_id`,`location_id` = DBT_INTERNAL_SOURCE.`location_id`,`device_code` = DBT_INTERNAL_SOURCE.`device_code`,`product_code` = DBT_INTERNAL_SOURCE.`product_code`,`company_code` = DBT_INTERNAL_SOURCE.`company_code`,`company_name` = DBT_INTERNAL_SOURCE.`company_name`,`task_location_info` = DBT_INTERNAL_SOURCE.`task_location_info`,`task_start_date` = DBT_INTERNAL_SOURCE.`task_start_date`,`telemetry_quantity` = DBT_INTERNAL_SOURCE.`telemetry_quantity`,`updated_at` = DBT_INTERNAL_SOURCE.`updated_at`,`created_at` = DBT_INTERNAL_SOURCE.`created_at`,`extracted_at` = DBT_INTERNAL_SOURCE.`extracted_at`
    

    when not matched then insert
        (`task_product_id`, `task_id`, `device_id`, `company_id`, `product_id`, `location_id`, `device_code`, `product_code`, `company_code`, `company_name`, `task_location_info`, `task_start_date`, `telemetry_quantity`, `updated_at`, `created_at`, `extracted_at`)
    values
        (`task_product_id`, `task_id`, `device_id`, `company_id`, `product_id`, `location_id`, `device_code`, `product_code`, `company_code`, `company_name`, `task_location_info`, `task_start_date`, `telemetry_quantity`, `updated_at`, `created_at`, `extracted_at`)


    