
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        device_id, valid_from
    from `evs-datastack-prod`.`prod_marts`.`dim_neshu__device_history`
    group by device_id, valid_from
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test