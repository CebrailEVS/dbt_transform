
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select type_code
from `evs-datastack-prod`.`prod_reference`.`ref_nesp_tech__key_type_inter`
where type_code is null



  
  
      
    ) dbt_internal_test