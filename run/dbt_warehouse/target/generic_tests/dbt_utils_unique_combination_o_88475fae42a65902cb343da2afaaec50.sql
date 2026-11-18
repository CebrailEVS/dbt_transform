
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        act_id, act_type
    from `evs-datastack-prod`.`prod_intermediate`.`int_nesp_co__activites`
    group by act_id, act_type
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test