
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        obj_vol_anneemois, obj_vol_key_item
    from `evs-datastack-prod`.`prod_reference`.`ref_nesp_tech__objectifs_volumes`
    group by obj_vol_anneemois, obj_vol_key_item
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test