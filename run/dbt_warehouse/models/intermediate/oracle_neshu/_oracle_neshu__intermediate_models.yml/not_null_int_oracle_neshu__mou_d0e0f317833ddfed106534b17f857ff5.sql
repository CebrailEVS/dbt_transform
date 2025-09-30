
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select task_product_id
from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__mouvement_interne_tasks`
where task_product_id is null



  
  
      
    ) dbt_internal_test