
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        company_id, device_id, product_id, location_id, location, consumption_date, data_source
    from `evs-datastack-prod`.`prod_marts`.`fct_oracle_neshu__conso_business_review`
    group by company_id, device_id, product_id, location_id, location, consumption_date, data_source
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test