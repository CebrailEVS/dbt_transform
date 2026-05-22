
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select mission_prevue
from `evs-datastack-prod`.`prod_marts`.`fct_neshu__passage_appro`
where mission_prevue is null



  
  
      
    ) dbt_internal_test