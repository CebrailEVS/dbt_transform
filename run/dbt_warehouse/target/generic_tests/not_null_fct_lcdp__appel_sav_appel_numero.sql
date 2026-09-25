
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select appel_numero
from `evs-datastack-prod`.`prod_marts`.`fct_lcdp__appel_sav`
where appel_numero is null



  
  
      
    ) dbt_internal_test