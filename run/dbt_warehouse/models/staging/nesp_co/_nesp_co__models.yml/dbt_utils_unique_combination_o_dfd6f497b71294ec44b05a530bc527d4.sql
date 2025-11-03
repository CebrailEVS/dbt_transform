
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        opportunity_id, employee_responsible, created_by
    from `evs-datastack-prod`.`prod_staging`.`stg_nesp_co__opportunite`
    group by opportunity_id, employee_responsible, created_by
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test