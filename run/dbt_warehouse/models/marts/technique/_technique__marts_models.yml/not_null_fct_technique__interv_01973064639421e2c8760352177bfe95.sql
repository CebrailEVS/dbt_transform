
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select flag_hors_delai_tech_effectif
from `evs-datastack-prod`.`prod_marts`.`fct_technique__intervention_retraitee`
where flag_hors_delai_tech_effectif is null



  
  
      
    ) dbt_internal_test