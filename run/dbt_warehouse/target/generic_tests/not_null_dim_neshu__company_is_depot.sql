
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select is_depot
from `evs-datastack-prod`.`prod_marts`.`dim_neshu__company`
where is_depot is null



  
  
      
    ) dbt_internal_test