
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select code_article
from `evs-datastack-prod`.`prod_marts`.`fct_technique__consommation_article_nespresso`
where code_article is null



  
  
      
    ) dbt_internal_test