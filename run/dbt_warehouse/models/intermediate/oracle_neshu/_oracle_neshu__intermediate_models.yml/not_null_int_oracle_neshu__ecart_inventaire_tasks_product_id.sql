
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select product_id
from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__ecart_inventaire_tasks`
where product_id is null



  
  
      
    ) dbt_internal_test