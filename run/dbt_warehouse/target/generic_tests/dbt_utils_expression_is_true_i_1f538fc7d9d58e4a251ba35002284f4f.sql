
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__stock_theorique`

where not(abs(stock_at_date - (stock_inventaire + plus - moins)) < 0.001)


  
  
      
    ) dbt_internal_test