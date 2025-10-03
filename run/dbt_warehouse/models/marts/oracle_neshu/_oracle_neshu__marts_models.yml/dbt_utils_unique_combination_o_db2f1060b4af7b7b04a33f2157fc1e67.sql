
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) >1 as should_error
    from (
      
    
  





with validation_errors as (

    select
        company_id
    from `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__contract`
    group by company_id
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test