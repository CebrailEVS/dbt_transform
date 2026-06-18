
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select movement_type
from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__chargement_tasks`
where movement_type is null



  
  
      
    ) dbt_internal_test