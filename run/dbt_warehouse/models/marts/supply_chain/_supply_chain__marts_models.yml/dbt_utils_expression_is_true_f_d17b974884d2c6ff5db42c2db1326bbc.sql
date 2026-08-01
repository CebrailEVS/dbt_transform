
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__point_commande_neshu`

where not(methode_prevision != 'exclu' or (quantite_a_commander = 0 and statut_reappro = 'exclu'))


  
  
      
    ) dbt_internal_test