
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select unique_id
from `evs-datastack-prod`.`prod_intermediate`.`int_dbt_artifacts__source_freshness`
where unique_id is null



  
  
      
    ) dbt_internal_test