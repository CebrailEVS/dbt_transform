
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select langage_code
from `evs-datastack-prod`.`prod_raw`.`lcdp_string`
where langage_code is null



  
  
      
    ) dbt_internal_test