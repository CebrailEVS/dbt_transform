
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select semaines_surstock_max
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__point_commande_neshu`
where semaines_surstock_max is null



  
  
      
    ) dbt_internal_test