
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select task_product_id
from `evs-datastack-prod`.`prod_export`.`exp_finance__neshu_reception`
where task_product_id is null



  
  
      
    ) dbt_internal_test