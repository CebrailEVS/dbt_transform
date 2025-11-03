
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        activity_id, employee_responsible, activity_type
    from `evs-datastack-prod`.`prod_staging`.`stg_nesp_co__activite`
    group by activity_id, employee_responsible, activity_type
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test