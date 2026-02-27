
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select contact_id
from `evs-datastack-prod`.`prod_staging`.`stg_yuman__contacts`
where contact_id is null



  
  
      
    ) dbt_internal_test