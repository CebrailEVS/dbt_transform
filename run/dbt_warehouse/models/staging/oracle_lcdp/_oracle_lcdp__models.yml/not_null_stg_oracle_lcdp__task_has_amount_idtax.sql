
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select idtax
from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__task_has_amount`
where idtax is null



  
  
      
    ) dbt_internal_test