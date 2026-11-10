
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__point_commande_neshu`

where not(quantite_a_commander_conditionnee * coeff_conditionnement >= quantite_a_commander)


  
  
      
    ) dbt_internal_test