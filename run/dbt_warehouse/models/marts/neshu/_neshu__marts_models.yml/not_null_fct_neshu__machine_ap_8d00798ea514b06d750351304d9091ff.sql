
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select nb_interventions_15j
from `evs-datastack-prod`.`prod_marts`.`fct_neshu__machine_appro_intervention`
where nb_interventions_15j is null



  
  
      
    ) dbt_internal_test