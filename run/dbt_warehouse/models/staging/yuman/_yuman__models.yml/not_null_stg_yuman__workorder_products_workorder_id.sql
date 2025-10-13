
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select workorder_id
from `evs-datastack-prod`.`prod_staging`.`stg_yuman__workorder_products`
where workorder_id is null



  
  
      
    ) dbt_internal_test