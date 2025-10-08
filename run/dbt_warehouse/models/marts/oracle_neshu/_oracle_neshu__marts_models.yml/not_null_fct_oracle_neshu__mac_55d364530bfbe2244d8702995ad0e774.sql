
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select status_inter
from `evs-datastack-prod`.`prod_marts`.`fct_oracle_neshu__machines_maintenance_tracking`
where status_inter is null



  
  
      
    ) dbt_internal_test