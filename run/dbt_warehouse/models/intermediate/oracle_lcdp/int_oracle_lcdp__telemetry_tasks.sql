
  
    

    create or replace table `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__telemetry_tasks`
      
    partition by timestamp_trunc(task_start_date, day)
    cluster by company_id, device_id, product_id

    
    OPTIONS(
      description="""Table interm\u00e9diaire des t\u00e2ches de t\u00e9l\u00e9m\u00e9trie. Filtr\u00e9e sur les t\u00e2ches de type t\u00e9l\u00e9m\u00e9trie (idtask_type=3) avec statut FAIT/VALIDE et label TELEM_SOURCE.\n"""
    )
    as (
      

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

    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__task` t

    -- Jointure obligatoire pour récupérer les produits
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__task_has_product` thp
        on thp.idtask = t.idtask

    -- Jointures pour enrichissement
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__company` c
        on c.idcompany = t.idcompany_peer

    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__device` d
        on d.iddevice = t.iddevice

    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__location` l
        on l.idlocation = t.idlocation

    -- Jointures pour le filtrage sur labels télémétrie
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__label_has_task` lht
        on t.idtask = lht.idtask

    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__label` la
        on lht.idlabel = la.idlabel

    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__label_family` lf
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


    );
  