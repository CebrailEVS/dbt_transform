
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select statut_vie
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__classification_article_neshu`
where statut_vie is null



  
  
      
    ) dbt_internal_test