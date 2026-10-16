
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        idresources, idlabel
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__v_label_resources`
    group by idresources, idlabel
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test