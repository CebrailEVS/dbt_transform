-- back compat for old kwarg name
  
  
        
            
            
            
            
        
    

    

    merge into `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__comptage_tasks` as DBT_INTERNAL_DEST
        using (
        select
        * from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__comptage_tasks__dbt_tmp`
        ) as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.task_id = DBT_INTERNAL_DEST.task_id))

    
    when matched then update set
        `task_id` = DBT_INTERNAL_SOURCE.`task_id`,`device_id` = DBT_INTERNAL_SOURCE.`device_id`,`company_id` = DBT_INTERNAL_SOURCE.`company_id`,`location_id` = DBT_INTERNAL_SOURCE.`location_id`,`device_code` = DBT_INTERNAL_SOURCE.`device_code`,`company_code` = DBT_INTERNAL_SOURCE.`company_code`,`company_name` = DBT_INTERNAL_SOURCE.`company_name`,`task_location_info` = DBT_INTERNAL_SOURCE.`task_location_info`,`task_start_date` = DBT_INTERNAL_SOURCE.`task_start_date`,`ca_pieces_eur` = DBT_INTERNAL_SOURCE.`ca_pieces_eur`,`ca_billets_eur` = DBT_INTERNAL_SOURCE.`ca_billets_eur`,`ca_titres_resto_eur` = DBT_INTERNAL_SOURCE.`ca_titres_resto_eur`,`ca_cash_eur` = DBT_INTERNAL_SOURCE.`ca_cash_eur`,`ca_cash_ht_eur` = DBT_INTERNAL_SOURCE.`ca_cash_ht_eur`,`ca_cash_tva_eur` = DBT_INTERNAL_SOURCE.`ca_cash_tva_eur`,`updated_at` = DBT_INTERNAL_SOURCE.`updated_at`,`created_at` = DBT_INTERNAL_SOURCE.`created_at`,`extracted_at` = DBT_INTERNAL_SOURCE.`extracted_at`
    

    when not matched then insert
        (`task_id`, `device_id`, `company_id`, `location_id`, `device_code`, `company_code`, `company_name`, `task_location_info`, `task_start_date`, `ca_pieces_eur`, `ca_billets_eur`, `ca_titres_resto_eur`, `ca_cash_eur`, `ca_cash_ht_eur`, `ca_cash_tva_eur`, `updated_at`, `created_at`, `extracted_at`)
    values
        (`task_id`, `device_id`, `company_id`, `location_id`, `device_code`, `company_code`, `company_name`, `task_location_info`, `task_start_date`, `ca_pieces_eur`, `ca_billets_eur`, `ca_titres_resto_eur`, `ca_cash_eur`, `ca_cash_ht_eur`, `ca_cash_tva_eur`, `updated_at`, `created_at`, `extracted_at`)


    