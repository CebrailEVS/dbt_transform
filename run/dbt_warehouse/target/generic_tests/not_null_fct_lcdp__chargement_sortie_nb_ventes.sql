
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select nb_ventes
from `evs-datastack-prod`.`prod_marts`.`fct_lcdp__chargement_sortie`
where nb_ventes is null



  
  
      
    ) dbt_internal_test