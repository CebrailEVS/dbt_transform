
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        src_inter, intervention_id, type_retraitement
    from `evs-datastack-prod`.`prod_intermediate`.`int_apptech__retraitements`
    group by src_inter, intervention_id, type_retraitement
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test