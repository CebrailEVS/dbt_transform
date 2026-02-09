
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select created_at
from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__inventaire_tasks`
where created_at is null



  
  
      
    ) dbt_internal_test