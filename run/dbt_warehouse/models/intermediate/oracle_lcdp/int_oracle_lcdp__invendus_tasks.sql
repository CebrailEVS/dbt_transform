
  
    

    create or replace table `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__invendus_tasks`
      
    partition by timestamp_trunc(task_start_date, day)
    cluster by device_id, product_id

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Invendus des distributeurs LCDP : une ligne par produit constat\u00e9 invendu et retir\u00e9 d'une machine lors d'une t\u00e2che INVENDUS (idtask_type 11). Mesure la casse / les pertes produit (p\u00e9remption, d\u00e9stockage) au grain device \u00d7 produit. Distinct des retraits de chargement (movement_type REMOVING de int_oracle_lcdp__chargement_tasks) : \u00e9v\u00e9nement de constat d\u00e9di\u00e9, pas de double comptage.\n[COMMENT CONSTRUITE] stg_oracle_lcdp__task (idtask_type = 11 INVENDUS, statuts FAIT/VALIDE, code_status_record = '1', real_start_date renseign\u00e9e) crois\u00e9 avec task_has_product (grain ligne produit), enrichi company/device/product/location. Quantit\u00e9 ramen\u00e9e en unit\u00e9s de base ; valorisation = quantit\u00e9 \u00d7 prix d'achat. Incr\u00e9mental (merge sur task_product_id).\n[GRAIN] 1 ligne par task_product_id (ligne produit d'une t\u00e2che invendus). ~4,9k lignes (~2k t\u00e2ches), 185 distributeurs, depuis d\u00e9cembre 2024.\n[NOTES] real_quantity toujours positive (quantit\u00e9 retir\u00e9e car invendue). Majoritairement des PRODUIT FRAIS (p\u00e9remption), puis SNACK et BOISSON FROIDE ; quelques lignes non vendables marginales (filtr\u00e9es en aval par le mart via ref_oracle_lcdp__product_group_vendable). Statuts FAIT/VALIDE uniquement (un invendu ne compte que s'il est r\u00e9alis\u00e9) \u2014 align\u00e9 sur la logique \u00ab event r\u00e9alis\u00e9 \u00bb de int_oracle_lcdp__telemetry_tasks.\n"""
    )
    as (
      

with invendus_tasks as (

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
        c.code as company_code,
        d.code as device_code,
        p.code as product_code,
        ts.code as task_status_code,

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

    where
        1 = 1
        and t.idtask_status in (1, 4)  -- FAIT, VALIDE (un invendu ne compte que s'il est réalisé)
        and t.code_status_record = '1'
        and t.idtask_type = 11  -- INVENDUS
        and t.real_start_date is not null
)

select * from invendus_tasks


    );
  