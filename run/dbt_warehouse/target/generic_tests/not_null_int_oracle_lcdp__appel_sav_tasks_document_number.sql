
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select document_number
from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__appel_sav_tasks`
where document_number is null



  
  
      
    ) dbt_internal_test