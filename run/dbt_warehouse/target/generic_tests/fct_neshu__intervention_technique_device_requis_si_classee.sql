
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_neshu__intervention_technique`

where not(device_id is not null or objet_intervent is null)


  
  
      
    ) dbt_internal_test