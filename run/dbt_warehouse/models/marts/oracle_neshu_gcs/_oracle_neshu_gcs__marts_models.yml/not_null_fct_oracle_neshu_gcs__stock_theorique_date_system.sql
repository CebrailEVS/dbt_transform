
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select date_system
from `evs-datastack-prod`.`prod_marts`.`fct_oracle_neshu_gcs__stock_theorique`
where date_system is null



  
  
      
    ) dbt_internal_test