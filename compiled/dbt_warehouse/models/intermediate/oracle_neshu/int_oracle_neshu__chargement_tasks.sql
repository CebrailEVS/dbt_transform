

with task_load_type as (

    -- Type de chargement agrégé au grain tâche (1 ligne par idtask) pour ne pas
    -- dédoubler le grain produit : une tâche peut porter plusieurs labels.
    select
        lht.idtask,
        logical_or(la.code = 'LOADING') as has_loading,
        logical_or(la.code = 'REMOVING') as has_removing
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_has_task` as lht
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label` as la on lht.idlabel = la.idlabel
    where la.code in ('LOADING', 'REMOVING')
    group by lht.idtask
),

chargement_base as (

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
        -- Type de chargement : le label est porté au grain tâche. Si une tâche
        -- porte les deux labels (anomalie ERP), on tranche au signe de la quantité.
        case
            when tlt.has_loading and tlt.has_removing
                then case when thp.real_quantity < 0 then 'REMOVING' else 'LOADING' end
            when tlt.has_removing then 'REMOVING'
            when tlt.has_loading then 'LOADING'
        end as load_type_code,

        -- Informations métier
        l.access_info as task_location_info,
        t.real_start_date as task_start_date,

        -- Informations de conditionnement
        thp.unit_coeff_multi,
        thp.unit_coeff_div,
        thp.real_quantity as base_unit_quantity,
        thp.net_price as product_unit_price_task,
        p.purchase_unit_price as product_unit_price_latest,

        -- Métriques
        sum(thp.real_quantity * thp.unit_coeff_multi / thp.unit_coeff_div) as load_quantity,
        sum(thp.real_quantity * thp.unit_coeff_multi / thp.unit_coeff_div) * p.purchase_unit_price as load_valuation,

        -- Timestamps techniques
        t.updated_at,
        t.created_at,
        t.extracted_at

    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task` as t
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_has_product` as thp on t.idtask = thp.idtask
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__company` as c on t.idcompany_peer = c.idcompany
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__device` as d on t.iddevice = d.iddevice
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__product` as p on thp.idproduct = p.idproduct
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__location` as l on t.idlocation = l.idlocation
    left join task_load_type as tlt on t.idtask = tlt.idtask
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_status` as ts on t.idtask_status = ts.idtask_status

    where
        1 = 1
        and t.idtask_status in (1, 4, 3, 5)  -- FAIT, VALIDE, ANNULE, ANOMALIE
        and t.code_status_record = '1'
        and t.idtask_type = 13 -- CHARGEMENT MACHINE
        and t.real_start_date is not null

    group by
        thp.idtask_has_product, t.idtask, t.iddevice, t.idcompany_peer,
        t.idproduct_source, t.type_product_source, t.idlocation,
        t.idproduct_destination, t.type_product_destination,
        thp.idproduct,
        c.code, d.code, p.code,
        ts.code, tlt.has_loading, tlt.has_removing,
        l.access_info, t.real_start_date,
        thp.unit_coeff_multi,
        thp.unit_coeff_div,
        thp.real_quantity,
        thp.net_price,
        p.purchase_unit_price,
        t.updated_at, t.created_at, t.extracted_at
),

ressources_roadman as (
    select
        thr.idtask,
        min(r.idresources) as roadman_id,
        min(r.code) as roadman_code
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_has_resources` as thr
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__resources` as r on thr.idresources = r.idresources
    where r.idresources_type = 2
    group by thr.idtask
),

ressources_vehicle as (
    select
        thr.idtask,
        min(r.idresources) as vehicle_id,
        min(r.code) as vehicle_code
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_has_resources` as thr
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__resources` as r on thr.idresources = r.idresources
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
    from chargement_base as cb
    left join ressources_roadman as rr on cb.task_id = rr.idtask
    left join ressources_vehicle as rv on cb.task_id = rv.idtask
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

    unit_coeff_multi,
    unit_coeff_div,
    base_unit_quantity,
    product_unit_price_task,
    product_unit_price_latest,

    -- Métriques
    load_quantity,
    load_valuation,

    -- Timestamps techniques
    updated_at,
    created_at,
    extracted_at

from chargement_enrichi


    where chargement_enrichi.updated_at >= (
        select max(t.updated_at) - interval 1 day
        from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__chargement_tasks` as t
    )
