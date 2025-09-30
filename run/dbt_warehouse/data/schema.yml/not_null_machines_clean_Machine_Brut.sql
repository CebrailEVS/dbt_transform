
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select Machine_Brut
from `evs-datastack-prod`.`prod_reference`.`machines_clean`
where Machine_Brut is null



  
  
      
    ) dbt_internal_test