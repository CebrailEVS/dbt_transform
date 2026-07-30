
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select device_id
from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__label_device`
where device_id is null



  
  
      
    ) dbt_internal_test