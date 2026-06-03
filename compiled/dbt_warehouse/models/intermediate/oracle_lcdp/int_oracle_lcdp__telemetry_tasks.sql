

with task_label_pivot as (
    select
        lht.idtask,
        max(case when lf.code = 'TELEM_SOURCE' then la.code end) as telemetry_source,
        max(case when lf.code = 'TELEM_SEND_CAUSE' then la.code end) as telemetry_send_cause
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__label_has_task` as lht
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__label` as la
        on lht.idlabel = la.idlabel
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__label_family` as lf
        on la.idlabel_family = lf.idlabel_family
    where lf.code in ('TELEM_SOURCE', 'TELEM_SEND_CAUSE')
    group by lht.idtask
),

telemetry_tasks as (
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

        -- Attributs télémétrie (labels pivotés)
        tlp.telemetry_source,
        tlp.telemetry_send_cause,

        -- Dates business
        t.real_start_date as task_start_date,

        -- Métrique business
        cast(1 as int64) as telemetry_quantity,  -- 1 Tâche = 1 unité de télémétrie

        -- Valorisation Nayax (prix de vente remonté par la télémétrie sur l'event)
        thp.sale_unit_price as sale_unit_price_eur,
        thp.sale_amount_net as sale_amount_net_eur,
        thp.sale_amount_net_tax as sale_amount_net_tax_eur,

        -- Timestamps techniques pour l'incrément
        t.updated_at,
        t.created_at,
        t.extracted_at

    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__task` as t

    -- Jointure obligatoire pour récupérer les produits
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__task_has_product` as thp
        on t.idtask = thp.idtask

    -- Filtrage sur la présence d'un label TELEM_SOURCE + récupération des valeurs
    inner join task_label_pivot as tlp
        on
            t.idtask = tlp.idtask
            and tlp.telemetry_source is not null

    -- Jointures pour enrichissement
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__company` as c
        on t.idcompany_peer = c.idcompany

    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__device` as d
        on t.iddevice = d.iddevice

    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__location` as l
        on t.idlocation = l.idlocation

    where 1 = 1
    -- Filtres business critiques
    and t.idtask_status in (1, 4)  -- FAIT et VALIDE uniquement
    and t.code_status_record = '1'   -- Enregistrement actif (string)
    and t.idtask_type = 3           -- Type télémétrie

    -- Filtre qualité données
    and t.real_start_date is not null  -- Éviter les tâches sans date de début
)

select * from telemetry_tasks


    where telemetry_tasks.updated_at >= (
        select max(t.updated_at) - interval 1 day
        from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__telemetry_tasks` as t
    )
