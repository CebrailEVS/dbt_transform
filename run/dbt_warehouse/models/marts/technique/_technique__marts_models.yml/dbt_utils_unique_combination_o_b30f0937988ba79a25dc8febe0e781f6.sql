
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        workorder_id, product_id
    from `evs-datastack-prod`.`prod_marts`.`fct_technique__workorder_product`
    group by workorder_id, product_id
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test