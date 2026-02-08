-- back compat for old kwarg name
  
  
        
            
	    
	    
            
        
    

    

    merge into `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__appro_tasks` as DBT_INTERNAL_DEST
        using (

with passage_appro_base as (

    select
        -- Identifiants
        t.idtask as task_id,
        t.iddevice as device_id,
        t.idcompany_peer as company_id,
        t.idproduct_source as product_source_id,
        t.type_product_source as product_source_type,
        t.idproduct_destination as product_destination_id,
        t.type_product_destination as product_destination_type,

        -- Codes
        c.code as company_code,

        -- Infos métier
        l.access_info as task_location_info,
        t.real_start_date as task_start_date,
        t.real_end_date as task_end_date,

        -- Status
        ts.code as task_status_code,

        -- Timestamps techniques
        t.updated_at,
        t.created_at,
        t.extracted_at

    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task` as t
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__company` as c on t.idcompany_peer = c.idcompany
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_status` as ts on t.idtask_status = ts.idtask_status
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__location` as l on t.idlocation = l.idlocation

    where
        1 = 1
        and t.idtask_type = 32 -- PASSAGE APPROVISIONNEURS
        and t.code_status_record = '1'
        and t.real_start_date is not null

    group by
        t.idtask, t.iddevice, t.idcompany_peer,
        t.idproduct_source, t.type_product_source,
        t.idproduct_destination, t.type_product_destination,
        ts.code, c.code, l.access_info,
        t.real_start_date, t.real_end_date,
        t.updated_at, t.created_at, t.extracted_at
)

select
    -- Identifiants
    task_id,
    device_id,
    company_id,
    product_source_id,
    product_destination_id,

    -- Codes
    company_code,

    -- Infos métier
    product_source_type,
    product_destination_type,
    task_location_info,
    task_status_code,
    task_start_date,
    task_end_date,

    -- Timestamps techniques
    updated_at,
    created_at,
    extracted_at

from passage_appro_base


    where passage_appro_base.updated_at >= (
        select max(updated_at) - interval 1 day  -- noqa: RF02
        from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__appro_tasks`
    )

        ) as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.task_id = DBT_INTERNAL_DEST.task_id))

    
    when matched then update set
        `task_id` = DBT_INTERNAL_SOURCE.`task_id`,`device_id` = DBT_INTERNAL_SOURCE.`device_id`,`company_id` = DBT_INTERNAL_SOURCE.`company_id`,`product_source_id` = DBT_INTERNAL_SOURCE.`product_source_id`,`product_destination_id` = DBT_INTERNAL_SOURCE.`product_destination_id`,`company_code` = DBT_INTERNAL_SOURCE.`company_code`,`product_source_type` = DBT_INTERNAL_SOURCE.`product_source_type`,`product_destination_type` = DBT_INTERNAL_SOURCE.`product_destination_type`,`task_location_info` = DBT_INTERNAL_SOURCE.`task_location_info`,`task_status_code` = DBT_INTERNAL_SOURCE.`task_status_code`,`task_start_date` = DBT_INTERNAL_SOURCE.`task_start_date`,`task_end_date` = DBT_INTERNAL_SOURCE.`task_end_date`,`updated_at` = DBT_INTERNAL_SOURCE.`updated_at`,`created_at` = DBT_INTERNAL_SOURCE.`created_at`,`extracted_at` = DBT_INTERNAL_SOURCE.`extracted_at`
    

    when not matched then insert
        (`task_id`, `device_id`, `company_id`, `product_source_id`, `product_destination_id`, `company_code`, `product_source_type`, `product_destination_type`, `task_location_info`, `task_status_code`, `task_start_date`, `task_end_date`, `updated_at`, `created_at`, `extracted_at`)
    values
        (`task_id`, `device_id`, `company_id`, `product_source_id`, `product_destination_id`, `company_code`, `product_source_type`, `product_destination_type`, `task_location_info`, `task_status_code`, `task_start_date`, `task_end_date`, `updated_at`, `created_at`, `extracted_at`)


    