
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        idtask, tax_rate, idtax, idtax_region
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_has_amount`
    group by idtask, tax_rate, idtax, idtax_region
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test