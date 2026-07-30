
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        company_id, idlabel
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__label_company`
    group by company_id, idlabel
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test