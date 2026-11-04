
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select numero_compte_general
from `evs-datastack-prod`.`prod_marts`.`fct_finance__pnl_section_mensuel`
where numero_compte_general is null



  
  
      
    ) dbt_internal_test