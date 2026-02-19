
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select n_planning
from `evs-datastack-prod`.`prod_intermediate`.`int_nesp_tech__interventions_dedup`
where n_planning is null



  
  
      
    ) dbt_internal_test