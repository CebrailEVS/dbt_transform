
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select resources_type
from `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__resources`
where resources_type is null



  
  
      
    ) dbt_internal_test