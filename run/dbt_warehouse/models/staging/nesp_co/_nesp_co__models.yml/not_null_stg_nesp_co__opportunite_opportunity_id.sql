
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select opportunity_id
from `evs-datastack-prod`.`prod_staging`.`stg_nesp_co__opportunite`
where opportunity_id is null



  
  
      
    ) dbt_internal_test