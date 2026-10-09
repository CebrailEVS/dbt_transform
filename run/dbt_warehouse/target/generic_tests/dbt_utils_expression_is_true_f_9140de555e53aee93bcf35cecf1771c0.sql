
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__prevision_demande_neshu`

where not(methode_prevision != 'exclu' or demande_prevue_mensuelle = 0)


  
  
      
    ) dbt_internal_test