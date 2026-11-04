
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select _dlt_id
from `evs-datastack-prod`.`prod_staging`.`stg_zoho_desk__agents`
where _dlt_id is null



  
  
      
    ) dbt_internal_test