
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select purchase_order_line_id
from `evs-datastack-prod`.`prod_staging`.`stg_yuman__purchase_orders`
where purchase_order_line_id is null



  
  
      
    ) dbt_internal_test