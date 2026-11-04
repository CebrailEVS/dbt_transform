
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select extracted_at
from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_historic`
where extracted_at is null



  
  
      
    ) dbt_internal_test