
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select task_status_code
from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__appro_tasks_enriched`
where task_status_code is null



  
  
      
    ) dbt_internal_test