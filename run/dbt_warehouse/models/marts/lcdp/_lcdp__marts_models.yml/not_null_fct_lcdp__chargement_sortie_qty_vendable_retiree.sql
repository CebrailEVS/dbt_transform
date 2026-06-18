
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select qty_vendable_retiree
from `evs-datastack-prod`.`prod_marts`.`fct_lcdp__chargement_sortie`
where qty_vendable_retiree is null



  
  
      
    ) dbt_internal_test