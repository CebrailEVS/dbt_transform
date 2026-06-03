
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select week_start_date
from `evs-datastack-prod`.`prod_marts`.`fct_lcdp__chargement_sortie`
where week_start_date is null



  
  
      
    ) dbt_internal_test