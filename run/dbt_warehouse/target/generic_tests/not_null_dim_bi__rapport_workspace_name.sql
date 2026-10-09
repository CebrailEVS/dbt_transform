
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select workspace_name
from `evs-datastack-prod`.`prod_marts`.`dim_bi__rapport`
where workspace_name is null



  
  
      
    ) dbt_internal_test