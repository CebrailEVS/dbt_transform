
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select updated_at
from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_has_amount`
where updated_at is null



  
  
      
    ) dbt_internal_test