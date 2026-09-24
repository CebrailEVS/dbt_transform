

-- Une tâche peut porter plusieurs ressources d'un même type. On retient UNE
-- ligne entière (celle du plus petit idresources) pour que l'identifiant et le
-- code désignent toujours la même ressource — même règle que
-- int_oracle_lcdp__chargement_tasks, pour que les deux flux d'un même passage
-- désignent le même roadman.
with ressources_roadman_ranked as (
    select
        thr.idtask,
        r.idresources as roadman_id,
        r.code as roadman_code,
        row_number() over (partition by thr.idtask order by r.idresources) as rn
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__task_has_resources` as thr
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__resources` as r on thr.idresources = r.idresources
    where r.idresources_type = 2
),

ressources_roadman as (
    select
        idtask,
        roadman_id,
        roadman_code
    from ressources_roadman_ranked
    where rn = 1
),

invendus_tasks as (

    select
        -- PK naturelle de task_has_product
        thp.idtask_has_product as task_product_id,

        -- IDs business
        t.idtask as task_id,
        t.iddevice as device_id,
        t.idcompany_peer as company_id,
        thp.idproduct as product_id,
        t.idlocation as location_id,
        rr.roadman_id,

        -- Codes métier pour les jointures futures
        c.code as company_code,
        d.code as device_code,
        p.code as product_code,
        ts.code as task_status_code,
        rr.roadman_code,

        -- Infos métier
        l.access_info as task_location_info,
        t.real_start_date as task_start_date,

        -- Conditionnement & prix
        thp.unit_coeff_multi,
        thp.unit_coeff_div,
        thp.real_quantity as base_unit_quantity,
        thp.net_price as product_unit_price_task,
        p.purchase_unit_price as product_unit_price_latest,

        -- Métriques (quantité ramenée en unités de base)
        thp.real_quantity * thp.unit_coeff_multi / thp.unit_coeff_div as quantity,
        thp.real_quantity * thp.unit_coeff_multi / thp.unit_coeff_div * p.purchase_unit_price as valuation,

        -- Timestamps techniques
        t.updated_at,
        t.created_at,
        t.extracted_at

    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__task` as t
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__task_has_product` as thp on t.idtask = thp.idtask
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__company` as c on t.idcompany_peer = c.idcompany
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__device` as d on t.iddevice = d.iddevice
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__product` as p on thp.idproduct = p.idproduct
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__location` as l on t.idlocation = l.idlocation
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__task_status` as ts on t.idtask_status = ts.idtask_status
    left join ressources_roadman as rr on t.idtask = rr.idtask

    where
        1 = 1
        and t.idtask_status in (1, 4)  -- FAIT, VALIDE (un invendu ne compte que s'il est réalisé)
        and t.code_status_record = '1'
        and t.idtask_type = 11  -- INVENDUS
        and t.real_start_date is not null
)

select * from invendus_tasks


    where invendus_tasks.updated_at >= (
        select max(t.updated_at) - interval 1 day
        from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__invendus_tasks` as t
    )
