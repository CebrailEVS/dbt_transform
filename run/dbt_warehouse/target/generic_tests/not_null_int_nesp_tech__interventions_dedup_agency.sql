
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select agency
from `evs-datastack-prod`.`prod_intermediate`.`int_nesp_tech__interventions_dedup`
where agency is null



  
  
      
    ) dbt_internal_test