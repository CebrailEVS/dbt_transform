
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select machine_clean
from `evs-datastack-prod`.`prod_intermediate`.`int_nesp_tech__facturation_interventions`
where machine_clean is null



  
  
      
    ) dbt_internal_test