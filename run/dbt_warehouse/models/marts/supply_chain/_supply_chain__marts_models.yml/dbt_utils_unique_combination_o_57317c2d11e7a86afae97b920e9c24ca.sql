
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        snapshot_date, entity_type, id_entity, product_code
    from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__stock_lcdp`
    group by snapshot_date, entity_type, id_entity, product_code
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test