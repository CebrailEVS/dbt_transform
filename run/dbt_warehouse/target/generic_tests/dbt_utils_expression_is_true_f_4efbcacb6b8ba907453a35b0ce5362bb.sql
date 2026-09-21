
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_technique__consommation_article_nespresso`

where not(qty_consommee > 0)


  
  
      
    ) dbt_internal_test