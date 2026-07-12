
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select classe_demande
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__classification_article_neshu`
where classe_demande is null



  
  
      
    ) dbt_internal_test