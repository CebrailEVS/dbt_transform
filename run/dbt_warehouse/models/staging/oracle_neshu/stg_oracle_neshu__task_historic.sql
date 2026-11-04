-- back compat for old kwarg name
  
  
        
    

    

    merge into `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_historic` as DBT_INTERNAL_DEST
        using (
        select
        * from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_historic__dbt_tmp132153619795`
        ) as DBT_INTERNAL_SOURCE
        on (FALSE)

    

    when not matched then insert
        (`idtask`, `idhistoric_action`, `idtask_status`, `idtask_type`, `idtask_source`, `iduser`, `idtreatment_by_batch`, `idinstance`, `iddevice`, `idcompany_peer`, `document_number`, `resources`, `custom_data`, `real_start_date`, `real_end_date`, `created_at`, `extracted_at`)
    values
        (`idtask`, `idhistoric_action`, `idtask_status`, `idtask_type`, `idtask_source`, `iduser`, `idtreatment_by_batch`, `idinstance`, `iddevice`, `idcompany_peer`, `document_number`, `resources`, `custom_data`, `real_start_date`, `real_end_date`, `created_at`, `extracted_at`)


    