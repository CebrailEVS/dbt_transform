
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select idstring
from `evs-datastack-prod`.`prod_raw`.`lcdp_string`
where idstring is null



  
  
      
    ) dbt_internal_test