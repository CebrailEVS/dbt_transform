
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        stock_date, stock, reference
    from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__technique_stock_van`
    group by stock_date, stock, reference
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test