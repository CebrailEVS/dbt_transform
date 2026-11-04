
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select material_is_active
from `evs-datastack-prod`.`prod_marts`.`dim_technique__material`
where material_is_active is null



  
  
      
    ) dbt_internal_test