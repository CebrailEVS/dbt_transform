
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        stock_date, depot, reference
    from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__rupture_depot_yuman`
    group by stock_date, depot, reference
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test