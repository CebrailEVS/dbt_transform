
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select quantite_excedentaire
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__point_commande_neshu`
where quantite_excedentaire is null



  
  
      
    ) dbt_internal_test