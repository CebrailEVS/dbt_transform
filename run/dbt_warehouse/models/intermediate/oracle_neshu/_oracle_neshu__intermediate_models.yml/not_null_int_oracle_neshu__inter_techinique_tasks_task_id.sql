
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select task_id
from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__inter_techinique_tasks`
where task_id is null



  
  
      
    ) dbt_internal_test