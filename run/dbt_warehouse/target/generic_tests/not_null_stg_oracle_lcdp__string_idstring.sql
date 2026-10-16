
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select idstring
from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__string`
where idstring is null



  
  
      
    ) dbt_internal_test