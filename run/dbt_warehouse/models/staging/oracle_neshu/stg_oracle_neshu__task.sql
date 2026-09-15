-- back compat for old kwarg name
  
  
        
            
            
            
            
        
    

    

    merge into `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task` as DBT_INTERNAL_DEST
        using (
        select
        * from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task__dbt_tmp140817989439`
        ) as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.idtask = DBT_INTERNAL_DEST.idtask))

    
    when matched then update set
        `idtask` = DBT_INTERNAL_SOURCE.`idtask`,`task_idtask` = DBT_INTERNAL_SOURCE.`task_idtask`,`idtask_type` = DBT_INTERNAL_SOURCE.`idtask_type`,`idtask_status` = DBT_INTERNAL_SOURCE.`idtask_status`,`iddevice` = DBT_INTERNAL_SOURCE.`iddevice`,`idcompany_peer` = DBT_INTERNAL_SOURCE.`idcompany_peer`,`idlocation` = DBT_INTERNAL_SOURCE.`idlocation`,`idcontact` = DBT_INTERNAL_SOURCE.`idcontact`,`idproduct_source` = DBT_INTERNAL_SOURCE.`idproduct_source`,`idproduct_destination` = DBT_INTERNAL_SOURCE.`idproduct_destination`,`type_product_source` = DBT_INTERNAL_SOURCE.`type_product_source`,`type_product_destination` = DBT_INTERNAL_SOURCE.`type_product_destination`,`xml` = DBT_INTERNAL_SOURCE.`xml`,`spantime` = DBT_INTERNAL_SOURCE.`spantime`,`code_status_record` = DBT_INTERNAL_SOURCE.`code_status_record`,`real_start_date` = DBT_INTERNAL_SOURCE.`real_start_date`,`real_end_date` = DBT_INTERNAL_SOURCE.`real_end_date`,`planed_start_date` = DBT_INTERNAL_SOURCE.`planed_start_date`,`planed_end_date` = DBT_INTERNAL_SOURCE.`planed_end_date`,`created_at` = DBT_INTERNAL_SOURCE.`created_at`,`updated_at` = DBT_INTERNAL_SOURCE.`updated_at`,`extracted_at` = DBT_INTERNAL_SOURCE.`extracted_at`
    

    when not matched then insert
        (`idtask`, `task_idtask`, `idtask_type`, `idtask_status`, `iddevice`, `idcompany_peer`, `idlocation`, `idcontact`, `idproduct_source`, `idproduct_destination`, `type_product_source`, `type_product_destination`, `xml`, `spantime`, `code_status_record`, `real_start_date`, `real_end_date`, `planed_start_date`, `planed_end_date`, `created_at`, `updated_at`, `extracted_at`)
    values
        (`idtask`, `task_idtask`, `idtask_type`, `idtask_status`, `iddevice`, `idcompany_peer`, `idlocation`, `idcontact`, `idproduct_source`, `idproduct_destination`, `type_product_source`, `type_product_destination`, `xml`, `spantime`, `code_status_record`, `real_start_date`, `real_end_date`, `planed_start_date`, `planed_end_date`, `created_at`, `updated_at`, `extracted_at`)


    