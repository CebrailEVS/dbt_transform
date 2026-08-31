
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_lcdp__ca_mensuel`

where not(ca_total_ttc_eur >= 0)


  
  
      
    ) dbt_internal_test