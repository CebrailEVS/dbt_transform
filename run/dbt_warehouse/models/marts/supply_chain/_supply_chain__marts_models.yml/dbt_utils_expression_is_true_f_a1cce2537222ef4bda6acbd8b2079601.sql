
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__stock_article_yuman`

where not(is_out_of_stock_depot = (depot_quantity = 0))


  
  
      
    ) dbt_internal_test