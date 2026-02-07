
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select iddevice
from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__label_has_device`
where iddevice is null



  
  
      
    ) dbt_internal_test