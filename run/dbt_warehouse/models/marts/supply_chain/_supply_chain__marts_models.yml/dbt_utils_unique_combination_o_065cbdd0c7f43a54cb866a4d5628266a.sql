
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        reference, stock_date
    from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__stock_article_yuman`
    group by reference, stock_date
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test