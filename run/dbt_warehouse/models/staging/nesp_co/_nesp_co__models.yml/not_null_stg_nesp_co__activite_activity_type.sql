
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select activity_type
from `evs-datastack-prod`.`prod_staging`.`stg_nesp_co__activite`
where activity_type is null



  
  
      
    ) dbt_internal_test