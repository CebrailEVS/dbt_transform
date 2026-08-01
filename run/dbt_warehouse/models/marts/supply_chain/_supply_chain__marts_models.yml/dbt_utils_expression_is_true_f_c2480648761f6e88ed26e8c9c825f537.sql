
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__point_commande_neshu`

where not(demande_retenue_journaliere >= demande_prevue_journaliere - 0.001)


  
  
      
    ) dbt_internal_test