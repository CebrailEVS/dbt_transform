
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select qty_invendus
from `evs-datastack-prod`.`prod_marts`.`fct_lcdp__mouvement_produit`
where qty_invendus is null



  
  
      
    ) dbt_internal_test