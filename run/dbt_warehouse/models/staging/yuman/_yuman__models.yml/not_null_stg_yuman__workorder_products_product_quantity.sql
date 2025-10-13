
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select product_quantity
from `evs-datastack-prod`.`prod_staging`.`stg_yuman__workorder_products`
where product_quantity is null



  
  
      
    ) dbt_internal_test