
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select mouvement_date
from `evs-datastack-prod`.`prod_marts`.`fct_lcdp__mouvement_produit`
where mouvement_date is null



  
  
      
    ) dbt_internal_test