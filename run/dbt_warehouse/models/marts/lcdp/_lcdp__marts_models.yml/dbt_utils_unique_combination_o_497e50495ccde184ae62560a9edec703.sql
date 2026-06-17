
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        device_id, month_start_date
    from `evs-datastack-prod`.`prod_marts`.`fct_lcdp__ca_mensuel`
    group by device_id, month_start_date
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test