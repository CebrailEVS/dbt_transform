
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select product_reference
from `evs-datastack-prod`.`prod_marts`.`fct_technique__consommation_article_nespresso`
where product_reference is null



  
  
      
    ) dbt_internal_test