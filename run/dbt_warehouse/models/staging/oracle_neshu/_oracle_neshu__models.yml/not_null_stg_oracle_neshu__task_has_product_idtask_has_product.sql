
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select idtask_has_product
from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_has_product`
where idtask_has_product is null



  
  
      
    ) dbt_internal_test