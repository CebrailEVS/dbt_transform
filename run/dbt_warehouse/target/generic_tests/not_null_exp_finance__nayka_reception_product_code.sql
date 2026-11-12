
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select product_code
from `evs-datastack-prod`.`prod_export`.`exp_finance__nayka_reception`
where product_code is null



  
  
      
    ) dbt_internal_test