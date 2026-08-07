
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        key_ref_inter, valid_from
    from `evs-datastack-prod`.`prod_reference`.`ref_nesp_tech__key_facturation`
    group by key_ref_inter, valid_from
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test