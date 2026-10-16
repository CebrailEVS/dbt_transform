
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select statut_reappro
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__point_commande_neshu`
where statut_reappro is null



  
  
      
    ) dbt_internal_test