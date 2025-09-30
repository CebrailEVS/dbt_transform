
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select material_id
from `evs-datastack-prod`.`prod_staging`.`stg_yuman__materials`
where material_id is null



  
  
      
    ) dbt_internal_test