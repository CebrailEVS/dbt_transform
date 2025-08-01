{{
    config(
        materialized='incremental',
        unique_key='task_product_id',
        partition_by={'field': 'task_start_date', 'data_type': 'timestamp'},
        incremental_strategy='merge',
        cluster_by=['company_id', 'device_id', 'product_id','task_status_code'],
        description='Table intermédiaire des tâches de chargement - avec enrichissement ressources (roadman, véhicule)'
    )
}}

with chargement_base as (

    select
        -- Identifiants
        thp.idtask_has_product as task_product_id,
        t.idtask as task_id,
        t.iddevice as device_id,
        t.idcompany_peer as company_id,
        t.idlocation as location_id,
        t.idproduct_source as product_source_id,
        t.type_product_source as product_source_type,
        t.idproduct_destination as product_destination_id,
        t.type_product_destination as product_destination_type,
        thp.idproduct as product_id,

        -- Codes
        c.code as company_code,
        d.code as device_code,
        p.code as product_code,
        ts.code as task_status_code,
        la.code as load_type_code,

        -- Informations métier
        l.access_info as task_location_info,
        t.real_start_date as task_start_date,

        -- Métriques
        sum(thp.real_quantity * thp.unit_coeff_multi / thp.unit_coeff_div) as load_quantity,
        sum(thp.real_quantity * thp.unit_coeff_multi / thp.unit_coeff_div) * thp.net_price as load_valuation,

        -- Timestamps techniques
        t.updated_at,
        t.created_at,
        t.extracted_at

    from {{ ref('stg_oracle_neshu__task') }} t
    inner join {{ ref('stg_oracle_neshu__task_has_product') }} thp on thp.idtask = t.idtask
    left join {{ ref('stg_oracle_neshu__company') }} c on c.idcompany = t.idcompany_peer
    left join {{ ref('stg_oracle_neshu__device') }} d on d.iddevice = t.iddevice
    left join {{ ref('stg_oracle_neshu__product') }} p on p.idproduct = thp.idproduct
    left join {{ ref('stg_oracle_neshu__location') }} l on l.idlocation = t.idlocation
    left join {{ ref('stg_oracle_neshu__label_has_task') }} lht on t.idtask = lht.idtask
    left join {{ ref('stg_oracle_neshu__label') }} la on lht.idlabel = la.idlabel
    left join {{ ref('stg_oracle_neshu__task_status') }} ts on t.idtask_status = ts.idtask_status

    where 1=1
        and t.idtask_status in (1, 4, 3)  -- FAIT, VALIDE, ANNULE
        and t.code_status_record = '1'
        and t.idtask_type = 13 -- CHARGEMENT MACHINE
        and t.real_start_date is not null

    group by
        thp.idtask_has_product, t.idtask, t.iddevice, t.idcompany_peer,
        t.idproduct_source, t.type_product_source, t.idlocation,
        t.idproduct_destination, t.type_product_destination,
        thp.idproduct, thp.net_price,
        c.code, d.code, p.code,
        ts.code, la.code,
        l.access_info, t.real_start_date,
        t.updated_at, t.created_at, t.extracted_at
),

ressources_roadman as (
    select 
        thr.idtask, 
        min(r.idresources) as roadman_id,
        min(r.code) as roadman_code
    from {{ ref('stg_oracle_neshu__task_has_resources') }} thr
    join {{ ref('stg_oracle_neshu__resources') }} r on r.idresources = thr.idresources
    where r.idresources_type = 2
    group by thr.idtask
),

ressources_vehicle as (
    select 
        thr.idtask, 
        min(r.idresources) as vehicle_id,
        min(r.code) as vehicle_code
    from {{ ref('stg_oracle_neshu__task_has_resources') }} thr
    join {{ ref('stg_oracle_neshu__resources') }} r on r.idresources = thr.idresources
    where r.idresources_type = 3
    group by thr.idtask
),

chargement_enrichi as (
    select 
        cb.*,
        rr.roadman_id,
        rr.roadman_code,
        rv.vehicle_id,
        rv.vehicle_code
    from chargement_base cb
    left join ressources_roadman rr on cb.task_id = rr.idtask
    left join ressources_vehicle rv on cb.task_id = rv.idtask
)

select
    -- Identifiants
    task_product_id,
    task_id,
    device_id,
    company_id,
    product_id,
    location_id,
    product_source_id,
    product_destination_id,
    roadman_id,
    vehicle_id,

    -- Codes
    company_code,
    device_code,
    product_code,
    roadman_code,
    vehicle_code,
    task_status_code,
    load_type_code,

    -- Infos métier
    product_source_type,
    product_destination_type,
    task_location_info,
    task_start_date,

    -- Métriques
    load_quantity,
    load_valuation,

    -- Timestamps techniques
    updated_at,
    created_at,
    extracted_at

from chargement_enrichi

{% if is_incremental() %}
  where updated_at >= (
      select max(updated_at) - interval 1 day
      from {{ this }}
  )
{% endif %}
