
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select prevision_retenue
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__erreur_prevision_neshu`
where prevision_retenue is null



  
  
      
    ) dbt_internal_test