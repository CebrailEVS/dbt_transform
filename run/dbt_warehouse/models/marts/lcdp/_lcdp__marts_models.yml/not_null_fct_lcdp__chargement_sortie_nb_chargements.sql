
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select nb_chargements
from `evs-datastack-prod`.`prod_marts`.`fct_lcdp__chargement_sortie`
where nb_chargements is null



  
  
      
    ) dbt_internal_test