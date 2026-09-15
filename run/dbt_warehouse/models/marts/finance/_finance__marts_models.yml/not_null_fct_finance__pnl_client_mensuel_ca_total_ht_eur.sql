
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select ca_total_ht_eur
from `evs-datastack-prod`.`prod_marts`.`fct_finance__pnl_client_mensuel`
where ca_total_ht_eur is null



  
  
      
    ) dbt_internal_test