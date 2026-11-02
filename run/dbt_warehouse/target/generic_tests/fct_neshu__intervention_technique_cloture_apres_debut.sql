
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_neshu__intervention_technique`

where not(date_done >= date_started)


  
  
      
    ) dbt_internal_test