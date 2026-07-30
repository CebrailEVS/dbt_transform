-- back compat for old kwarg name
  
  
        
            
            
            
            
        
    

    

    merge into `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__invendus_tasks` as DBT_INTERNAL_DEST
        using (

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


    where invendus_tasks.updated_at >= (
        select max(t.updated_at) - interval 1 day
        from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__invendus_tasks` as t
    )

        ) as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.task_product_id = DBT_INTERNAL_DEST.task_product_id))

    
    when matched then update set
        `task_product_id` = DBT_INTERNAL_SOURCE.`task_product_id`,`task_id` = DBT_INTERNAL_SOURCE.`task_id`,`device_id` = DBT_INTERNAL_SOURCE.`device_id`,`company_id` = DBT_INTERNAL_SOURCE.`company_id`,`product_id` = DBT_INTERNAL_SOURCE.`product_id`,`location_id` = DBT_INTERNAL_SOURCE.`location_id`,`company_code` = DBT_INTERNAL_SOURCE.`company_code`,`device_code` = DBT_INTERNAL_SOURCE.`device_code`,`product_code` = DBT_INTERNAL_SOURCE.`product_code`,`task_status_code` = DBT_INTERNAL_SOURCE.`task_status_code`,`task_location_info` = DBT_INTERNAL_SOURCE.`task_location_info`,`task_start_date` = DBT_INTERNAL_SOURCE.`task_start_date`,`unit_coeff_multi` = DBT_INTERNAL_SOURCE.`unit_coeff_multi`,`unit_coeff_div` = DBT_INTERNAL_SOURCE.`unit_coeff_div`,`base_unit_quantity` = DBT_INTERNAL_SOURCE.`base_unit_quantity`,`product_unit_price_task` = DBT_INTERNAL_SOURCE.`product_unit_price_task`,`product_unit_price_latest` = DBT_INTERNAL_SOURCE.`product_unit_price_latest`,`quantity` = DBT_INTERNAL_SOURCE.`quantity`,`valuation` = DBT_INTERNAL_SOURCE.`valuation`,`updated_at` = DBT_INTERNAL_SOURCE.`updated_at`,`created_at` = DBT_INTERNAL_SOURCE.`created_at`,`extracted_at` = DBT_INTERNAL_SOURCE.`extracted_at`
    

    when not matched then insert
        (`task_product_id`, `task_id`, `device_id`, `company_id`, `product_id`, `location_id`, `company_code`, `device_code`, `product_code`, `task_status_code`, `task_location_info`, `task_start_date`, `unit_coeff_multi`, `unit_coeff_div`, `base_unit_quantity`, `product_unit_price_task`, `product_unit_price_latest`, `quantity`, `valuation`, `updated_at`, `created_at`, `extracted_at`)
    values
        (`task_product_id`, `task_id`, `device_id`, `company_id`, `product_id`, `location_id`, `company_code`, `device_code`, `product_code`, `task_status_code`, `task_location_info`, `task_start_date`, `unit_coeff_multi`, `unit_coeff_div`, `base_unit_quantity`, `product_unit_price_task`, `product_unit_price_latest`, `quantity`, `valuation`, `updated_at`, `created_at`, `extracted_at`)


    