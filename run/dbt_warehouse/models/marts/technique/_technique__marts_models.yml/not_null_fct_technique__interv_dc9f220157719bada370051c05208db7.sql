
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select statut_facturation_effectif
from `evs-datastack-prod`.`prod_marts`.`fct_technique__intervention_retraitee`
where statut_facturation_effectif is null



  
  
      
    ) dbt_internal_test