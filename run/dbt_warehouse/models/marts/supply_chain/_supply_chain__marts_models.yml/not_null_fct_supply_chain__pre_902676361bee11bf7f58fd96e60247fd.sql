
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select demande_prevue_mensuelle
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__prevision_demande_neshu`
where demande_prevue_mensuelle is null



  
  
      
    ) dbt_internal_test