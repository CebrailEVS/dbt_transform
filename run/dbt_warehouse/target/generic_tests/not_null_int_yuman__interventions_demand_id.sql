
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select demand_id
from `evs-datastack-prod`.`prod_intermediate`.`int_yuman__interventions`
where demand_id is null



  
  
      
    ) dbt_internal_test