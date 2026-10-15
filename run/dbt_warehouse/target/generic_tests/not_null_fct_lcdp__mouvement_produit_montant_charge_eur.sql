
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select montant_charge_eur
from `evs-datastack-prod`.`prod_marts`.`fct_lcdp__mouvement_produit`
where montant_charge_eur is null



  
  
      
    ) dbt_internal_test