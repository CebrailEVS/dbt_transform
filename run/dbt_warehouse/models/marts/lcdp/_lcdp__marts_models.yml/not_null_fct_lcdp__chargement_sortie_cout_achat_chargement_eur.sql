
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select cout_achat_chargement_eur
from `evs-datastack-prod`.`prod_marts`.`fct_lcdp__chargement_sortie`
where cout_achat_chargement_eur is null



  
  
      
    ) dbt_internal_test