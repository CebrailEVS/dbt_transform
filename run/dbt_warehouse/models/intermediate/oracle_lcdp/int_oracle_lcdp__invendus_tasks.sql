-- back compat for old kwarg name
  
  
        
            
            
            
            
        
    

    

    merge into `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__invendus_tasks` as DBT_INTERNAL_DEST
        using (
        select
        * from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__invendus_tasks__dbt_tmp101813907501`
        ) as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.task_product_id = DBT_INTERNAL_DEST.task_product_id))

    
    when matched then update set
        `task_product_id` = DBT_INTERNAL_SOURCE.`task_product_id`,`task_id` = DBT_INTERNAL_SOURCE.`task_id`,`device_id` = DBT_INTERNAL_SOURCE.`device_id`,`company_id` = DBT_INTERNAL_SOURCE.`company_id`,`product_id` = DBT_INTERNAL_SOURCE.`product_id`,`location_id` = DBT_INTERNAL_SOURCE.`location_id`,`roadman_id` = DBT_INTERNAL_SOURCE.`roadman_id`,`company_code` = DBT_INTERNAL_SOURCE.`company_code`,`device_code` = DBT_INTERNAL_SOURCE.`device_code`,`product_code` = DBT_INTERNAL_SOURCE.`product_code`,`task_status_code` = DBT_INTERNAL_SOURCE.`task_status_code`,`roadman_code` = DBT_INTERNAL_SOURCE.`roadman_code`,`task_location_info` = DBT_INTERNAL_SOURCE.`task_location_info`,`task_start_date` = DBT_INTERNAL_SOURCE.`task_start_date`,`unit_coeff_multi` = DBT_INTERNAL_SOURCE.`unit_coeff_multi`,`unit_coeff_div` = DBT_INTERNAL_SOURCE.`unit_coeff_div`,`base_unit_quantity` = DBT_INTERNAL_SOURCE.`base_unit_quantity`,`product_unit_price_task` = DBT_INTERNAL_SOURCE.`product_unit_price_task`,`product_unit_price_latest` = DBT_INTERNAL_SOURCE.`product_unit_price_latest`,`quantity` = DBT_INTERNAL_SOURCE.`quantity`,`valuation` = DBT_INTERNAL_SOURCE.`valuation`,`updated_at` = DBT_INTERNAL_SOURCE.`updated_at`,`created_at` = DBT_INTERNAL_SOURCE.`created_at`,`extracted_at` = DBT_INTERNAL_SOURCE.`extracted_at`
    

    when not matched then insert
        (`task_product_id`, `task_id`, `device_id`, `company_id`, `product_id`, `location_id`, `roadman_id`, `company_code`, `device_code`, `product_code`, `task_status_code`, `roadman_code`, `task_location_info`, `task_start_date`, `unit_coeff_multi`, `unit_coeff_div`, `base_unit_quantity`, `product_unit_price_task`, `product_unit_price_latest`, `quantity`, `valuation`, `updated_at`, `created_at`, `extracted_at`)
    values
        (`task_product_id`, `task_id`, `device_id`, `company_id`, `product_id`, `location_id`, `roadman_id`, `company_code`, `device_code`, `product_code`, `task_status_code`, `roadman_code`, `task_location_info`, `task_start_date`, `unit_coeff_multi`, `unit_coeff_div`, `base_unit_quantity`, `product_unit_price_task`, `product_unit_price_latest`, `quantity`, `valuation`, `updated_at`, `created_at`, `extracted_at`)


    