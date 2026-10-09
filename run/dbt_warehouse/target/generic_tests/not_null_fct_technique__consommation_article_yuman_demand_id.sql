
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select demand_id
from `evs-datastack-prod`.`prod_marts`.`fct_technique__consommation_article_yuman`
where demand_id is null



  
  
      
    ) dbt_internal_test