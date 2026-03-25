
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select obj_vol_alias_type_inter
from `evs-datastack-prod`.`prod_reference`.`ref_nesp_tech__objectifs_volumes`
where obj_vol_alias_type_inter is null



  
  
      
    ) dbt_internal_test