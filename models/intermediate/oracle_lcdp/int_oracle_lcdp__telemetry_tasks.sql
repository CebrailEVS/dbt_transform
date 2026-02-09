{{
    config(
        materialized='incremental',
        unique_key='task_product_id',
        partition_by={'field': 'task_start_date', 'data_type': 'timestamp'},
        incremental_strategy='merge',
        cluster_by=['company_id', 'device_id', 'product_id'],
        description='Table intermédiaire des tâches de télémétrie - Filtrée sur type télémétrie avec labels TELEM_SOURCE'
    )
}}

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

    from {{ ref('stg_oracle_lcdp__task') }} as t

    -- Jointure obligatoire pour récupérer les produits
    inner join {{ ref('stg_oracle_lcdp__task_has_product') }} as thp
        on t.idtask = thp.idtask

    -- Jointures pour enrichissement
    left join {{ ref('stg_oracle_lcdp__company') }} as c
        on t.idcompany_peer = c.idcompany

    left join {{ ref('stg_oracle_lcdp__device') }} as d
        on t.iddevice = d.iddevice

    left join {{ ref('stg_oracle_lcdp__location') }} as l
        on t.idlocation = l.idlocation

    -- Jointures pour le filtrage sur labels télémétrie
    inner join {{ ref('stg_oracle_lcdp__label_has_task') }} as lht
        on t.idtask = lht.idtask

    inner join {{ ref('stg_oracle_lcdp__label') }} as la
        on lht.idlabel = la.idlabel

    inner join {{ ref('stg_oracle_lcdp__label_family') }} as lf
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

{% if is_incremental() %}
    where telemetry_tasks.updated_at >= (
        select max(t.updated_at) - interval 1 day
        from {{ this }} as t
    )
{% endif %}
