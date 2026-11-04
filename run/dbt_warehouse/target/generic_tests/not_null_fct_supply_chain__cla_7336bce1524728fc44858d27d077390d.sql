
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select date_calcul
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__classification_article_neshu`
where date_calcul is null



  
  
      
    ) dbt_internal_test