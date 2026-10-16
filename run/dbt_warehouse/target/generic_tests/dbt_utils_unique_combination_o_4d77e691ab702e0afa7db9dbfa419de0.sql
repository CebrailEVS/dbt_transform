
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        idcompany, idlocation, idlocation_type
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__company_has_location`
    group by idcompany, idlocation, idlocation_type
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test