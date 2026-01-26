
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select machine_clean
from `evs-datastack-prod`.`prod_reference`.`ref_nesp_tech__machines_clean`
where machine_clean is null



  
  
      
    ) dbt_internal_test