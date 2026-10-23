
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select quantite_a_commander_conditionnee
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__point_commande_neshu`
where quantite_a_commander_conditionnee is null



  
  
      
    ) dbt_internal_test