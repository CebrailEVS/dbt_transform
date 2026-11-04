
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select execution_name
from `evs-datastack-prod`.`prod_intermediate`.`int_dbt_artifacts__node_result`
where execution_name is null



  
  
      
    ) dbt_internal_test