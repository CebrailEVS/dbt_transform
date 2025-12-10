
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select reference
from `evs-datastack-prod`.`prod_marts`.`fct_yuman_gcs__stock_articles`
where reference is null



  
  
      
    ) dbt_internal_test