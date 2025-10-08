
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select company_name
from `evs-datastack-prod`.`prod_marts`.`fct_oracle_neshu__machines_maintenance_tracking`
where company_name is null



  
  
      
    ) dbt_internal_test