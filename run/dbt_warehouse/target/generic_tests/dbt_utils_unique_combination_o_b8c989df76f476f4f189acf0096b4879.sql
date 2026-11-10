
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        snapshot_date, entity_type, id_entity, product_code
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__stock_theorique`
    group by snapshot_date, entity_type, id_entity, product_code
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test