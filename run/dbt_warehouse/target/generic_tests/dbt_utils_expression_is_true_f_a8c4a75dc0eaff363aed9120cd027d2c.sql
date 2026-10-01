
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__erreur_prevision_neshu`

where not(prevision_retenue >= 0 and prevision_moyenne_mobile >= 0 and prevision_saisonniere >= 0)


  
  
      
    ) dbt_internal_test