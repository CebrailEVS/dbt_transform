
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select source_last_preventive
from `evs-datastack-prod`.`prod_marts`.`fct_technique__neshu_maintenance_preventives`
where source_last_preventive is null



  
  
      
    ) dbt_internal_test