
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select source_last_preventive
from `evs-datastack-prod`.`prod_marts`.`fct_neshu__maintenance_preventive`
where source_last_preventive is null



  
  
      
    ) dbt_internal_test