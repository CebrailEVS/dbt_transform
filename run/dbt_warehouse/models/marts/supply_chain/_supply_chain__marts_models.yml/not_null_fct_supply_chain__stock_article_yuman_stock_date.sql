
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select stock_date
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__stock_article_yuman`
where stock_date is null



  
  
      
    ) dbt_internal_test