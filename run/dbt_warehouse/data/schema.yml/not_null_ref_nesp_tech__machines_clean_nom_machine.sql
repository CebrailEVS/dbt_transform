
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select nom_machine
from `evs-datastack-prod`.`prod_reference`.`ref_nesp_tech__machines_clean`
where nom_machine is null



  
  
      
    ) dbt_internal_test