
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select n_planning
from `evs-datastack-prod`.`prod_staging`.`stg_nesp_tech__articles`
where n_planning is null



  
  
      
    ) dbt_internal_test