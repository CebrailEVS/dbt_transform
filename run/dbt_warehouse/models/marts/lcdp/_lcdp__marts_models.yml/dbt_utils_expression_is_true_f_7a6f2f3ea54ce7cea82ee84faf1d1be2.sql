
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_lcdp__chargement_sortie`

where not(qty_chargee >= 0)


  
  
      
    ) dbt_internal_test