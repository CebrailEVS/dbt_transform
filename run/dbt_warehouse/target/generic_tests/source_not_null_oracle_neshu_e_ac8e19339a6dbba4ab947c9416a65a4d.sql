
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select idtreatment_by_batch
from `evs-datastack-prod`.`prod_raw`.`evs_treatment_by_batch`
where idtreatment_by_batch is null



  
  
      
    ) dbt_internal_test