
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select status
from `evs-datastack-prod`.`prod_intermediate`.`int_dbt_artifacts__source_freshness`
where status is null



  
  
      
    ) dbt_internal_test