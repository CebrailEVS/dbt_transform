
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select task_historic_id
from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__task_historic_metier`
where task_historic_id is null



  
  
      
    ) dbt_internal_test