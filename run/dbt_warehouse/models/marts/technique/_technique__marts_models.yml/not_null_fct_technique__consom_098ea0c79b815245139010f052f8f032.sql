
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select n_planning
from `evs-datastack-prod`.`prod_marts`.`fct_technique__consommation_article_nespresso`
where n_planning is null



  
  
      
    ) dbt_internal_test