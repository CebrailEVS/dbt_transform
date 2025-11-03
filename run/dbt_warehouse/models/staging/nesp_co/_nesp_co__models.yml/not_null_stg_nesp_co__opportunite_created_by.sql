
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select created_by
from `evs-datastack-prod`.`prod_staging`.`stg_nesp_co__opportunite`
where created_by is null



  
  
      
    ) dbt_internal_test