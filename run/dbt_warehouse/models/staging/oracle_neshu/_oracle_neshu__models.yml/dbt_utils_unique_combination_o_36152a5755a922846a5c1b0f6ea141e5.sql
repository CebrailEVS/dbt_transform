
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        idcontact, iddevice
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__contact_has_device`
    group by idcontact, iddevice
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test