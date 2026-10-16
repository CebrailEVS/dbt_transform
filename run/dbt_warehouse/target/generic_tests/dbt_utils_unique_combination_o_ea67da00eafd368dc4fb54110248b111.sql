
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        idlabel_family, code
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__label`
    group by idlabel_family, code
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test